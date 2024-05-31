import itertools
from datetime import timedelta, datetime
from jinja2 import Environment, meta, Template

class ConfigGenerator:
    DATE_FORMAT = '%Y-%m-%d'
    TARGET_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
    
    def __init__(self, timezone_offset=0):
        self.env = Environment()
        self.timezone_offset = timezone_offset

    def parse_date(self, date_str, format_str=None):
        """Parse date string to datetime object."""
        format_str = format_str or self.DATE_FORMAT
        return datetime.strptime(date_str, format_str)

    def calculate_dates(self, input_date_str, offset_days):
        """Calculate start and end dates based on input date and offset."""
        input_date = self.parse_date(input_date_str)
        end_date = input_date
        start_date = input_date - timedelta(days=offset_days)
        return start_date.strftime(self.DATE_FORMAT), end_date.strftime(self.DATE_FORMAT)

    def check_date_range(self, start_date, end_date):
        """Checks manual start and end dates."""
        if start_date > end_date:
            raise ValueError("Start date should be less than or equal to end date")

    def generate_date_range(self, start_date_str, end_date_str):
        """Generate a range of dates between start and end date."""
        start_date = self.parse_date(start_date_str)
        end_date = self.parse_date(end_date_str)
        
        current_date = start_date
        while current_date <= end_date:
            next_date = current_date + timedelta(days=1)
            yield (
                (current_date.strftime(self.DATE_FORMAT)),
                (current_date.strftime(self.DATE_FORMAT))
                # (current_date - timedelta(hours=self.timezone_offset)).strftime(self.TARGET_FORMAT),
                # (next_date - timedelta(hours=self.timezone_offset)).strftime(self.TARGET_FORMAT)
            )
            current_date = next_date

    def get_template_values(self, request_kwargs):
        """Extract template variables from request kwargs."""
        template_variables = set()
        
        for key, value in request_kwargs.items():
            try:
                parsed_content = self.env.parse(value)
                variables = meta.find_undeclared_variables(parsed_content)
                template_variables.update(variables)
            except Exception as e:
                print(f"Error parsing template for key {key}: {e}")
        
        return template_variables

    def clean_additional_keys(self, additional_keys, request_kwargs_args_keys):
        """Clean additional keys based on request kwargs args keys."""
        return {k: v for k, v in additional_keys.items() if k in request_kwargs_args_keys}

    def render_value(self, value, additional_keys=None):
        """Render value with Jinja2 template."""
        additional_keys = additional_keys or {}
        if isinstance(value, dict):
            return {
                subkey: Template(subvalue).render(**additional_keys) if isinstance(subvalue, str) else subvalue
                for subkey, subvalue in value.items()
            }
        elif isinstance(value, str):
            return Template(value).render(**additional_keys)
        else:
            return value

    def render_kwargs(self, request_kwargs, 
                      offset_days=15,
                      refresh_start_date=None, 
                      refresh_end_date=None, 
                      additional_keys=None):
        """Render request kwargs with additional keys."""
        
        today = datetime.today().strftime(self.DATE_FORMAT)

        if not (refresh_start_date and refresh_end_date):
            execution_date = today
            refresh_start_date, refresh_end_date = self.calculate_dates(execution_date, offset_days)
        else:
            self.check_date_range(refresh_start_date, refresh_end_date)

        request_kwargs_args_values = self.get_template_values(request_kwargs)
        cleaned_additional_keys = self.clean_additional_keys(additional_keys or {}, request_kwargs_args_values)
        required_values = ["start_date", "end_date"]

        keys_to_combine = list(cleaned_additional_keys.keys())

        if all(item in request_kwargs_args_values for item in required_values):
            keys_to_combine = keys_to_combine + list(required_values)
            zipped_period_lists = list(self.generate_date_range(refresh_start_date, refresh_end_date))
        else:
            zipped_period_lists = []

        values_to_combine = list(cleaned_additional_keys.values())
        if zipped_period_lists:
            values_to_combine.append(zipped_period_lists)

        combinations = list(itertools.product(*values_to_combine))

        rendered_kwargs_list = []
        for combination in combinations:
            flat_combination = [item for sublist in combination for item in (sublist if isinstance(sublist, (list, tuple)) else [sublist])]
            current_additional_keys = dict(zip(keys_to_combine, flat_combination))
            current_additional_keys['execution_dt'] = current_additional_keys.get('end_date', today).split('T')[0]
            request_kwargs["execution_dt"] = current_additional_keys['execution_dt']

            combined_dict = {}
            for key, value in request_kwargs.items():
                combined_dict[key] = self.render_value(value, current_additional_keys)
            
            rendered_kwargs_list.append(combined_dict)
            
        return rendered_kwargs_list