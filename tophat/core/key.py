class Key(set):
    """
    Implements a key for a table.
    A key is a set of (eventually one) MetadataFields.
    """

class Keys(set):
    """
    Implements a set of keys for a table.
    
    """

    def get_field_names(self):
        """
        \brief Returns a set of fields making up one key. If multiple possible
        keys exist, a unique one is returned, having a minimal size.
        """
        return min(self, key=len)
            
            
