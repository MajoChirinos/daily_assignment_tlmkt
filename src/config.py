"""Config management for daily assignment system."""


def _parse_bool(value):
    """Parse common boolean string/int representations."""
    if isinstance(value, bool):
        return value

    text = str(value).strip().lower()
    if text in ('true', '1', 'yes', 'y', 'si', 's'):
        return True
    if text in ('false', '0', 'no', 'n'):
        return False

    raise ValueError(f"Cannot parse bool value: {value}")

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
                if var_value is None or str(var_value).strip() == '':
                    parsed_list = []
                else:
                    parsed_list = [item.strip() for item in str(var_value).split(',') if item.strip()]
                setattr(self, var_name, parsed_list)
            elif var_type == 'dict(str,list(str))':
                # Format: "PEN:sport_events,reactivation|BOB:sport_events"
                # Empty value -> empty dict
                parsed_dict = {}
                if var_value is not None and str(var_value).strip():
                    for entry in str(var_value).split('|'):
                        entry = entry.strip()
                        if not entry:
                            continue
                        if ':' not in entry:
                            raise ValueError(
                                f"Invalid dict entry '{entry}' for variable '{var_name}'. "
                                "Expected format: 'KEY:val1,val2'"
                            )
                        key, _, values_str = entry.partition(':')
                        key = key.strip()
                        values = [v.strip() for v in values_str.split(',') if v.strip()]
                        parsed_dict[key] = values
                setattr(self, var_name, parsed_dict)
            elif var_type == 'bool':
                setattr(self, var_name, _parse_bool(var_value))
            else:
                raise ValueError(f"Unknown type: {var_type}")
    
    def __repr__(self):
        """Show config for debugging."""
        attrs = [f"{k}={v}" for k, v in self.__dict__.items() if not k.startswith('_')]
        return f"Config({', '.join(attrs)})"
    
    def to_dict(self):
        """Convert to dictionary."""
        return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}
