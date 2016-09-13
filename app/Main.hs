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
import qualified Data.Trie as T
import System.Environment (getArgs)
import Control.Monad.Trans.Resource (ResourceT, transResourceT, runResourceT)
import Control.Monad.Trans.Either (EitherT, bimapEitherT, runEitherT)
import Control.DeepSeq (force)
import Data.Foldable (for_)
import Text.Printf (printf)
import Data.Char (ord)
import Data.Either (isRight)

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

csvPipeline :: FilePath -> ConduitM BS.ByteString (Either CsvStreamRecordParseError [BS.ByteString]) M ()
csvPipeline fileName = fromCsvStreamError options NoHeader (CsvError fileName)
  where options = DecodeOptions { decDelimiter = fromIntegral (ord '?') }

pipeline :: FilePath -> ConduitM i o M [T.Trie ()]
pipeline fileName = sourceFile fileName =$=
                    tarBz2Pipeline =$=
                    iterMC (liftIO . putStrLn . fst) =$=
                    concatMapMC runCsvPipeline =$=
                    foldlC (force . zipWith setInsert) emptySets
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
        setInsert s e = T.insert e () s
        emptySets = repeat T.empty

processResults res = for_ (zip [(1 :: Int)..] res) $ \(i, s) -> do
 putStrLn (printf "%03d -> %d" i (T.size s))
 let pipeline = sourceList (T.keys s) =$=
                mapC (flip BS.snoc (fromIntegral (ord '\n'))) =$=
                sinkFile (printf "%03d.txt" i)
 runResourceT (runConduit pipeline)

main :: IO ()
main = do
  [fileName] <- getArgs
  res <- runEitherT $ runResourceT $ runConduit $ pipeline fileName
  either print processResults res
