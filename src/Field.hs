module Field where

import Data.Serialize (Get, encode, Serialize (..), Putter)
import qualified Data.ByteString as B
import Data.Int (Int32)

data Field
  = FieldBool Bool
  | FieldInt32 Int32
  deriving (Show, Eq)

-- We don't use the generic Serialize implementation as it prepends
-- encoded values with the index of the data constructor in the sum
-- type definition.
putField :: Putter Field
putField (FieldBool b)
  = put b
putField (FieldInt32 n)
  = put n

data FieldType
  = FieldTypeBool
  | FieldTypeInt32
  deriving (Show)

-- Byte size of each field
fieldTypeSize :: FieldType -> Int
fieldTypeSize FieldTypeBool
  = 1
fieldTypeSize FieldTypeInt32
  = 4

decodeField :: FieldType -> Get Field
decodeField FieldTypeBool
  = FieldBool <$> get
decodeField FieldTypeInt32
  = FieldInt32 <$> get

type FieldSpec = [FieldType]

fieldSpecSize :: FieldSpec -> Int
fieldSpecSize
  = sum . map fieldTypeSize
