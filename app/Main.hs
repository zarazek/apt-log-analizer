{-# LANGUAGE ExistentialQuantification #-}

module Main where
import Data.Conduit (ConduitM, (=$=), transPipe, runConduit)
import Data.Conduit.List (sourceList)
import Conduit
import Data.Conduit.BZlib (bunzip2)
import Codec.Archive.Tar (conduitEntry, FormatError)
import Codec.Archive.Tar.Entry (entryPath, entryContent, EntryContent(..), FileSize)
import Data.CSV.Conduit (intoCSV, CSVSettings(..), Row)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Trie as T
import System.Environment (getArgs)
import Control.Monad.Trans.Resource (ResourceT, transResourceT, runResourceT)
import Control.Monad.Trans.Either (EitherT, bimapEitherT, runEitherT)
import Control.Monad.Catch (throwM)
import Control.Exception (Exception, displayException, AssertionFailed(..))
import Control.DeepSeq (force)
import Data.Foldable (for_)
import Text.Printf (printf)
import Data.Char (ord)


data PipelineError = TarError FormatError
  deriving Show

type M = ResourceT (EitherT PipelineError IO)

transformError f = transResourceT (bimapEitherT f id)

tarBz2Pipeline :: ConduitM BS.ByteString (FilePath, FileSize, LBS.ByteString) M ()
tarBz2Pipeline = bunzip2 =$=
                 transPipe (transformError TarError) conduitEntry =$=
                 filterC (isNormalFile . entryContent) =$=
                 mapC retrieveContents
  where isNormalFile (NormalFile _ _) = True
        isNormalFile  Directory       = False
        isNormalFile  _               = error "unexpected entry type"
        retrieveContents e = (entryPath e, fileSize c, fileContents c)
          where fileSize (NormalFile _ s) = s
                fileSize _                = error "fileSize: not an ordinary file"
                fileContents (NormalFile c _) = c
                fileContents _                = error "fileContents: not an ordinary file"
                c = entryContent e

csvPipeline :: ConduitM BS.ByteString (Row BS.ByteString) M ()
csvPipeline = intoCSV options
  where options = CSVSettings { csvSep = '?', csvQuoteChar = Nothing }

pipeline :: FilePath -> ConduitM i o M [T.Trie ()]
pipeline fileName = sourceFile fileName =$=
                    tarBz2Pipeline =$=
                    mapMC (\t -> liftIO (putStrLn (first t)) >> return (third t)) =$=
                    -- (throwM (AssertionFailed "dupa") >> concatMapMC runCsvPipeline) =$=
                    concatMapMC runCsvPipeline =$=

                    foldlC (force . zipWith setInsert) emptySets 
  where first (x, _, _) = x
        third (_, _, x) = x
        runCsvPipeline lbs = runConduit pipe
          where pipe = sourceLazy lbs =$= csvPipeline =$= sinkList
        setInsert s e = T.insert e () s
        emptySets = repeat T.empty

processResults res = for_ (zip [(1 :: Int)..] res) $ \(i, s) -> do
 putStrLn (show i)
 let pipeline = sourceList (T.keys s) =$=
                mapC (flip BS.snoc (fromIntegral (ord '\n'))) =$=
                sinkFile (printf "%03d.txt" i)
 runResourceT (runConduit pipeline)

main :: IO ()
main = do
  [fileName] <- getArgs
  res <- runEitherT $ runResourceT $ runConduit $ pipeline fileName
  either print processResults res
