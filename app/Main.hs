{-# LANGUAGE BangPatterns, OverloadedStrings, ScopedTypeVariables #-}

module Main where
import Data.Conduit (ConduitM, (=$=), transPipe, runConduit)
import Data.Conduit.List (sourceList)
import Conduit
import Data.Conduit.BZlib (bunzip2)
import Codec.Archive.Tar (conduitEntry, FormatError)
import Codec.Archive.Tar.Entry (entryPath, entryContent, EntryContent(..))
import Data.Csv (DecodeOptions(..), HasHeader(..))
import Data.Csv.Conduit (CsvStreamHaltParseError, CsvStreamRecordParseError, fromCsvStreamError)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Trie as M
import System.Environment (getArgs)
import Control.Monad.Trans.Resource (ResourceT, transResourceT, runResourceT)
import Control.Monad.Trans.Either (EitherT, bimapEitherT, runEitherT)
import Control.DeepSeq (NFData(..), force)
import Data.Foldable (for_)
import Text.Printf (printf)
import Data.Char (ord)
import Data.Either (isRight)
import Data.List (intersperse)

import Data.Aeson ((.:))
import qualified Data.Aeson as A
import qualified Data.Aeson.Types as A (typeMismatch)
import qualified Data.Text as T

data PipelineError = TarError FormatError
                   | CsvError FilePath CsvStreamHaltParseError
 deriving Show

type M = ResourceT (EitherT PipelineError IO)

transformError f = transResourceT (bimapEitherT f id)

tarBz2Pipeline :: ConduitM BS.ByteString (FilePath, LBS.ByteString) M ()
tarBz2Pipeline = bunzip2 =$=
                 transPipe (transformError TarError) conduitEntry =$=
                 filterC (isNormalFile . entryContent) =$=
                 mapC retrieveContents
  where isNormalFile (NormalFile _ _) = True
        isNormalFile  Directory       = False
        isNormalFile  _               = error "unexpected entry type"
        retrieveContents e = (entryPath e, fileContents (entryContent e))
          where fileContents (NormalFile c _) = c
                fileContents _                = error "fileContents: not an ordinary file"

data ColumnType = CTMicrosecondTimestamp
                | CTText
                | CTInt

instance A.FromJSON ColumnType where
  parseJSON (A.String str) = case str of
                              "microsecond_timestamp" -> pure CTMicrosecondTimestamp
                              "text"                  -> pure CTText
                              "int"                   -> pure CTInt
                              _                       -> fail $ concat ["unknown column type '", T.unpack str, "'"]
  parseJSON invalid      = A.typeMismatch "ColumnType" invalid

data ColumnDescription = ColumnDescription { cdIndex    :: Word
                                           , cdName     :: T.Text
                                           , cdType     :: ColumnType
                                           , cdNullable :: Bool }

instance A.FromJSON ColumnDescription where
  parseJSON (A.Object obj) = ColumnDescription <$>
                             obj .: "idx" <*>
                             obj .: "name" <*>
                             obj .: "type" <*>
                             obj .: "nullable"
  parseJSON invalid       = A.typeMismatch "ColumnDescription" invalid

data CsvFormat = CsvFormat { cfSeparator          :: Char
                           , cfNumOfColumns       :: Word
                           , cfInterestingColumns :: [ColumnDescription] }

instance A.FromJSON CsvFormat where
  parseJSON (A.Object obj) = (CsvFormat <$>
                              obj .: "separator" <*>
                              obj .: "num_of_columns" <*>
                              obj .: "columns")
                             >>= checkColumnIndices
    where checkColumnIndices fmt = case foldr checkColumnIdx [] (cfInterestingColumns fmt) of
                                     []   -> return fmt
                                     errs -> fail (formatErrors errs)
            where checkColumnIdx col errs = case idx < numOfColumns of
                                              True  -> errs
                                              False -> (cdName col, idx) : errs
                    where idx = cdIndex col
                  formatErrors = concat . intersperse "\n" . map formatError
                  formatError (name, idx) = printf "Invalid index of column '%s': %d (valid are [0-%d])"
                                           (T.unpack name) idx (numOfColumns - 1)
                  numOfColumns = cfNumOfColumns fmt

csvPipeline :: FilePath -> ConduitM BS.ByteString (Either CsvStreamRecordParseError [BS.ByteString]) M ()
csvPipeline fileName = fromCsvStreamError options NoHeader (CsvError fileName)
  where options = DecodeOptions { decDelimiter = fromIntegral (ord '?') }

instance NFData (M.Trie a) where
  rnf !t = ()

pipeline :: FilePath -> ConduitM i o M [M.Trie ()]
pipeline fileName = sourceFile fileName =$=
                    tarBz2Pipeline =$=
                    iterMC (liftIO . putStrLn . fst) =$=
                    concatMapMC runCsvPipeline =$=
                    foldlC addToSets emptySets
  where runCsvPipeline (name, lbs) = runConduit pipe
          where pipe = sourceLazy lbs =$=
                       csvPipeline name =$=
                       iterMC printError =$=
                       filterC isRight =$=
                       mapC fromRight =$=
                       sinkList
                printError x = case x of
                  Left e  -> liftIO $ putStrLn (name ++ ": " ++ show e)
                  Right _ -> return ()
        fromRight (Right x) = x
        addToSets ss ee = force (zipWith setInsert ss ee)
        setInsert s e = M.insert e () s
        emptySets = repeat M.empty

processResults res = for_ (zip [(1 :: Int)..] res) $ \(i, s) -> do
 putStrLn (printf "%03d -> %d" i (M.size s))
 let pipeline = sourceList (M.keys s) =$=
                mapC (flip BS.snoc (fromIntegral (ord '\n'))) =$=
                sinkFile (printf "%03d.txt" i)
 runResourceT (runConduit pipeline)

main :: IO ()
main = do
  [confFile, inputFile] <- getArgs
  (config :: Either String CsvFormat) <- A.eitherDecode' <$> LBS.readFile confFile
  case config of
    Left err -> putStrLn err
    _        -> do
                  res <- runEitherT $ runResourceT $ runConduit $ pipeline inputFile
                  either print processResults res
