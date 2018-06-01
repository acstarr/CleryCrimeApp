-- Adam Starr
-- Feb. 8 2018

-- Assumes Claremont College Daily Crime logs converted from PFD using the pdftotext command from poppler

{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}


module LogParser where
import           Control.Exception             (IOException)
import qualified Control.Exception             as Exception
import           Control.Monad                 (join)
import qualified Control.Monad                 as Monad
import           Data.ByteString.Char8         (pack)
import           Data.ByteString.Lazy          (ByteString)
import qualified Data.ByteString.Lazy          as ByteString
import           Data.Char
import qualified Data.Csv                      as Cassava
import           Data.Either.Unwrap            (fromRight)
import qualified Data.Foldable                 as Foldable
import           Data.List                     (intercalate, nub, sort)
import           Data.List.Split               (wordsBy)
import           Data.Time.Calendar
import           Data.Time.LocalTime
import           GHC.Generics
import           System.Environment
import           System.Exit
import           Text.ParserCombinators.Parsec


strip :: String -> String
strip = f . f
   where f = reverse . dropWhile isSpace

data LogEntry = LogEntry { code         :: String
                         , crime        :: String
                         , description  :: Maybe String
                         , college      :: String
                         , location     :: Maybe String
                         , address      :: Maybe String
                         , dateReported :: Maybe Day
                         , timeReported :: Maybe TimeOfDay
                         , dateStarted  :: Maybe Day
                         , timeStarted  :: Maybe TimeOfDay
                         , dateEnded    :: Maybe Day
                         , timeEnded    :: Maybe TimeOfDay
                         , caseNumber   :: String
                         , disposition  :: String
                         } deriving (Generic, Show)




instance Cassava.ToField Day where
  toField = pack . showGregorian
instance Cassava.ToField TimeOfDay where
  toField = pack . show
instance Cassava.ToNamedRecord LogEntry
instance Cassava.DefaultOrdered LogEntry

{--
instance ToNamedRecord LogEntry where
    toNamedRecord (LogEntry code crime description college location address dateReported timeReported startDate startTime endDate endTime caseNumber disposition) = namedRecord [
        "code" .= code, "crime" .= crime ]


instance Show Date where
    show Nothing = "Null"
    show Just (m,d,y) = Text.Printf.printf "%04d\n" y ++ "-" Text.Printf.printf "%02d\n" m ++ "-" Text.Printf.printf "%02d\n" d
-}

log :: GenParser Char st [LogEntry]
log =  do pages <- many page
          eof
          return $ join pages

page :: GenParser Char st [LogEntry]
page = do entries <- many incident
          eof
          return entries


pCode :: GenParser Char st String
pCode = many $ satisfy (\c-> isAlphaNum c || (c== '.') || (c== '(') || (c== ')') || (c== '/') || (c== ' '))

pCrime :: GenParser Char st String
pCrime = many $ satisfy (\c-> isAlphaNum c || (c== ' ')|| (c== '/'))

pDescription :: GenParser Char st (Maybe String)
pDescription =(Just <$> try (char '-' *> spaces *> many (satisfy (\c-> (c/='\n') && isAlphaNum c || isSeparator c || isSymbol c || isPunctuation c))))<|> pure Nothing

pCollege :: GenParser Char st String
pCollege = choice $ map (try.string) ["POM", "SCR", "PTZ", "KGI", "CMC", "HMC", "CGU", "CUC", "OFF CAMPUS"]

pLocation :: GenParser Char st (Maybe String)
pLocation = (Just <$> try (char ':' *> spaces *> many (satisfy (\c-> (c/=':') && (isAlphaNum c || isSeparator c || isSymbol c || isPunctuation c))))) <|> pure Nothing



pAddress :: GenParser Char st (Maybe String)
pAddress = (Just <$> try (char ':' *> spaces *> many (satisfy (/= '\n')))) <|> pure Nothing

dateTime :: GenParser Char st (Maybe Day, Maybe TimeOfDay)
dateTime = try (string "UNKNOWN") *> pure (Nothing,Nothing) <|> do theDate <- date
                                                                   theTime <- time
                                                                   return (theDate,theTime)


date :: GenParser Char st (Maybe Day)
date = do month <- many digit
          _ <- char '/'
          day <- many digit
          _ <- char '/'
          year <- many digit
          return $ fromGregorianValid (read year) (read month) (read day)

hourto24 :: Int -> Bool -> Int
hourto24 h am = case h of 12 -> if am then 0 else 12
                          _  -> if am then h else h + 12


time :: GenParser Char st (Maybe TimeOfDay)
time = do spaces
          h <- many digit
          _ <- char ':'
          m <- many digit
          spaces
          isAM <- try (string "AM" *> pure True) <|> string "PM" *> pure False
          return $ makeTimeOfDayValid (hourto24 (read h) isAM) (read m) 0

pDispositionLine :: GenParser Char st String
pDispositionLine = many1 (satisfy (\c-> isAlphaNum c || (c == '/') || (c== ' ')|| (c== '-') || (c== ',')|| (c== '(') || (c== ')'))) <* char '\n'


pDisposition :: GenParser Char st String
pDisposition = unwords <$> many pDispositionLine



pageNum :: GenParser Char st ()
pageNum = do spaces
             _ <- string "Page"
             spaces
             _ <- many digit
             spaces
             _ <- string "of"
             spaces
             _ <- many digit
             return ()


pCaseNumber :: GenParser Char st String
pCaseNumber = many $ satisfy (\c -> isAlphaNum c || c == '-')

incident :: GenParser Char st LogEntry
incident =  do spaces
               _ <- string "Incident Type:"
               spaces
               optional $ try (string "CLAREMONT COLLEGES :")
               spaces
               theCode <- pCode
               spaces
               _ <- char ':'
               spaces
               theCrime <- pCode
               spaces
               theDescription <- pDescription
               spaces
               _ <- string "Location:"
               spaces
               theCollege <- pCollege
               spaces
               theLoc <- pLocation
               spaces
               theAddress <- pAddress
               spaces
               _ <- string "Date/Time Reported:"
               spaces
               (reportDate, reportTime) <- dateTime
               spaces
               _ <- string "Incident Occurred Between:"
               spaces
               (startDate, startTime) <- dateTime
               spaces
               _ <- string "and"
               spaces
               (endDate, endTime) <- dateTime
               spaces
               _ <- string "Case #:"
               spaces
               theCaseNum <- pCaseNumber
               spaces
               _ <- string "Int. Ref. #:"
               spaces
               _ <- string "Disposition:"
               theDispo <- pDisposition
               optional $ try pageNum                      -- end of line
               return LogEntry { code = strip theCode
                                 , crime = strip theCrime
                                 , description = strip <$> theDescription
                                 , college = strip theCollege
                                 , location = strip <$> theLoc
                                 , address = strip <$> theAddress
                                 , dateReported = reportDate
                                 , timeReported =reportTime
                                 , dateStarted = startDate
                                 , timeStarted = startTime
                                 , dateEnded = endDate
                                 , timeEnded = endTime
                                 , caseNumber = strip theCaseNum
                                 , disposition = strip theDispo
                                 }

-- The end of line character is \n
eol :: GenParser Char st Char
eol = char '\n'

parseLog :: String -> Either ParseError [LogEntry]
parseLog = parse LogParser.page "error"

catchShowIO
  :: IO a
  -> IO (Either String a)
catchShowIO action =
  fmap Right action
    `Exception.catch` handleIOException
  where
    handleIOException
      :: IOException
      -> IO (Either String a)
    handleIOException =
      return . Left . show

encodeItems
  :: [LogEntry]
  -> ByteString
encodeItems =
  Cassava.encodeDefaultOrderedByName . Foldable.toList

encodeItemsToFile
  :: FilePath
  -> [LogEntry]
  -> IO (Either String ())
encodeItemsToFile filePath =
  catchShowIO . ByteString.writeFile filePath . encodeItems

cleanDispo :: LogEntry -> LogEntry
cleanDispo l = let cleanDisposition = intercalate "," $ sort $ nub $ filter (not . null) $ map strip $ wordsBy  (\c-> c=='/' || c==',') $ disposition l
                 in l{disposition=cleanDisposition}


main :: IO ()
main = do
  args <- getArgs
  theLog <- if last args == "-"
    then getContents
    else readFile $ last args
  --print $ parseLog theLog
  Monad.void (encodeItemsToFile "../data/log.csv" $ map cleanDispo (fromRight$ parseLog theLog))
  exitSuccess
