"""Config management for daily assignment system."""

class Config:
    """Manages configuration parameters from DataFrame."""
    
    def __init__(self, conf_df):
        """Initialize from DataFrame with columns: variable, value, type."""
        self._load_from_dataframe(conf_df)
    
    def _load_from_dataframe(self, conf_df):
        """Load and convert configuration values by type."""
        for _, row in conf_df.iterrows():
            var_name = row['variable']
            var_value = row['value']
            var_type = row['type']
            
            # Convert value according to specified type
            if var_type == 'int':
                setattr(self, var_name, int(var_value))
            elif var_type == 'float':
                setattr(self, var_name, float(var_value))
            elif var_type == 'str':
                setattr(self, var_name, str(var_value))
            elif var_type == 'list(str)':
                setattr(self, var_name, var_value.split(', '))
            else:
                raise ValueError(f"Unknown type: {var_type}")
    
    def __repr__(self):
        """Show config for debugging."""
        attrs = [f"{k}={v}" for k, v in self.__dict__.items() if not k.startswith('_')]
        return f"Config({', '.join(attrs)})"
    
    def to_dict(self):
        """Convert to dictionary."""
        return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}
