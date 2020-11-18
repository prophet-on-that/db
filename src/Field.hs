module Field where

import Data.Serialize (Get, Serialize (..), Putter)
import Data.Int (Int32)
import GHC.Generics (Generic)

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
  deriving (Show, Generic)

instance Serialize FieldType

-- Byte size of each field
fieldTypeSize :: FieldType -> Int
fieldTypeSize FieldTypeBool
  = 1
fieldTypeSize FieldTypeInt32
  = 4

getField :: FieldType -> Get Field
getField FieldTypeBool
  = FieldBool <$> get
getField FieldTypeInt32
  = FieldInt32 <$> get

type FieldSpec = [FieldType]

fieldSpecSize :: FieldSpec -> Int
fieldSpecSize
  = sum . map fieldTypeSize

validateFields :: FieldSpec -> [Field] -> Bool
validateFields [] []
  = True
validateFields (spec : specs) (field : fields)
  = validateField spec field && validateFields specs fields
  where
    validateField FieldTypeBool (FieldBool _)
      = True
    validateField FieldTypeInt32 (FieldInt32 _)
      = True
    validateField _ _
      = False
validateFields _ _
  = False
