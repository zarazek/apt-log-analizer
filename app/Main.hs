{-# LANGUAGE BangPatterns, OverloadedStrings, ScopedTypeVariables #-}

module Main where
import Data.Conduit (ConduitM, (=$=), transPipe, runConduit)
import Data.Conduit.List (sourceList)
import Conduit
import Data.Conduit.BZlib (bunzip2)
import Codec.Archive.Tar (conduitEntry, FormatError)
import Codec.Archive.Tar.Entry (entryPath, entryContent, EntryContent(..))
import Data.Csv (DecodeOptions(..), HasHeader(..), Record, Parser, parseField)
import Data.Csv.Conversion (parseUnsigned)
import Data.Csv.Conduit (CsvStreamHaltParseError, CsvStreamRecordParseError, fromCsvStreamError, fromCsvStreamError')
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
import Data.List (intersperse, foldl')

import Data.Aeson ((.:))
import qualified Data.Aeson as A
import qualified Data.Aeson.Types as A (typeMismatch)
import qualified Data.Text as T

import Data.Time.Clock (UTCTime)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import Data.Ratio ((%))

import qualified Data.HashMap.Strict as HM
import qualified Data.Vector as V

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

data ColumnDescription = ColumnDescription { cdIndex    :: Int
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
                           , cfNumOfColumns       :: Int
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

parseMicrosecondTimestamp :: BS.ByteString -> Parser UTCTime
parseMicrosecondTimestamp bs = convert <$> parseUnsigned "Number of microseconds since 1970.01.01 00:00:00" bs
  where convert us = posixSecondsToUTCTime (realToFrac (us % 1000000))

parseNullable :: (BS.ByteString -> Parser a) -> BS.ByteString -> Parser (Maybe a)
parseNullable _ "NULL" = pure Nothing
parseNullable p bs     = Just <$> p bs

data CsvValue = CsvTimestamp UTCTime
              | CsvNullableTimestamp (Maybe UTCTime)
              | CsvText T.Text
              | CsvNullableText (Maybe T.Text)
              | CsvInt Int
              | CsvNullableInt (Maybe Int)
  deriving (Eq, Show)

parseCsvValue :: ColumnType -> Bool -> BS.ByteString -> Parser CsvValue
parseCsvValue ctype nullable = case (ctype, nullable) of
                                 (CTMicrosecondTimestamp, False) -> \bs -> CsvTimestamp <$> parseMicrosecondTimestamp bs
                                 (CTMicrosecondTimestamp, True)  -> \bs -> CsvNullableTimestamp <$> parseNullable parseMicrosecondTimestamp bs
                                 (CTText, False)                 -> \bs -> CsvText <$> parseField bs
                                 (CTText, True)                  -> \bs -> CsvNullableText <$> parseNullable parseField bs
                                 (CTInt, False)                  -> \bs -> CsvInt <$> parseUnsigned "Positive integer" bs
                                 (CTInt, True)                   -> \bs -> CsvNullableInt <$> parseNullable (parseUnsigned "Positive inteter") bs

type CsvRecord = HM.HashMap T.Text CsvValue

parseCsvRecord :: Int -> [ColumnDescription] -> Record -> Parser CsvRecord
parseCsvRecord numOfCols colDscs = parser
  where parser allCols = case len == numOfCols of
                            True  -> do
                                       parsedCols <- sequence $ zipWith ($) colParsers selectedCols
                                       return $ foldIntoRecord parsedCols
                            False -> fail $ printf "Expected %d fields, got %d" numOfCols len
          where len = V.length allCols
                selectedCols = V.toList $ V.backpermute allCols indices
        colParsers = map makeColParser colDscs
        makeColParser colDsc =  parseCsvValue (cdType colDsc) (cdNullable colDsc)
        indices = V.fromList (map cdIndex colDscs)
        names = map cdName colDscs
        foldIntoRecord vals = foldl' addToRecord HM.empty $ zip names vals
        addToRecord m (name, value) = HM.insert name value m

csvPipeline :: FilePath -> ConduitM BS.ByteString (Either CsvStreamRecordParseError [BS.ByteString]) M ()
csvPipeline fileName = fromCsvStreamError options NoHeader (CsvError fileName)
  where options = DecodeOptions { decDelimiter = fromIntegral (ord '?') }

newCsvPipeline :: CsvFormat -> FilePath -> ConduitM BS.ByteString (Either CsvStreamRecordParseError CsvRecord) M ()
newCsvPipeline format fileName = fromCsvStreamError' parser options NoHeader (CsvError fileName)
  where parser = parseCsvRecord (cfNumOfColumns format) (cfInterestingColumns format)
        options = DecodeOptions { decDelimiter = fromIntegral $ ord $ cfSeparator format }

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

addNewLine bs = BS.snoc bs newLineChar
  where newLineChar = fromIntegral (ord '\n')

processResults res = for_ (zip [(1 :: Int)..] res) $ \(i, s) -> do
 putStrLn (printf "%03d -> %d" i (M.size s))
 let pipeline = sourceList (M.keys s) =$=
                mapC addNewLine =$=
                sinkFile (printf "%03d.txt" i)
 runResourceT (runConduit pipeline)

newPipeline :: CsvFormat -> FilePath -> FilePath -> ConduitM i o M ()
newPipeline config inputFile outputFile = sourceFile inputFile =$=
                                          tarBz2Pipeline =$=
                                          iterMC (liftIO . putStrLn . fst) =$=
                                          concatMapMC runCsvPipeline =$=
                                          mapC (addNewLine . BS.pack . map (fromIntegral . ord) . show) =$=
                                          sinkFile outputFile
  where runCsvPipeline (name, lbs) = runConduit pipe
          where pipe = sourceLazy lbs =$=
                       cvsPipe name =$=
                       iterMC printError =$=
                       filterC isRight =$=
                       mapC fromRight =$=
                       sinkList
                printError x = case x of
                  Left e  -> liftIO $ putStrLn (name ++ ": " ++ show e)
                  Right _ -> return ()
        cvsPipe = newCsvPipeline config
        fromRight (Right x) = x
        addToSets ss ee = force (zipWith setInsert ss ee)
        setInsert s e = M.insert e () s
        emptySets = repeat M.empty

main :: IO ()
main = do
  [confFile, inputFile, outputFile] <- getArgs
  maybeConfig <- A.eitherDecode' <$> LBS.readFile confFile
  case maybeConfig of
    Left err     -> putStrLn err
    Right config -> do
                      res1 <- runEitherT $ runResourceT $ runConduit $ pipeline inputFile
                      either print processResults res1
                      res2 <- runEitherT $ runResourceT $ runConduit $ newPipeline config inputFile outputFile
                      either print return res2
