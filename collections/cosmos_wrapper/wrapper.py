from logging import error
import azure.cosmos.cosmos_client as cosmos_client
from datetime import datetime
import re

class NeoRheo():
    def __init__(self, HOST, MASTER_KEY, DATABASE_ID):
        self.client = cosmos_client.CosmosClient(HOST, {'masterKey': MASTER_KEY}, user_agent="neorheo", user_agent_overwrite=True)
        self.db = self.client.get_database_client(DATABASE_ID)
        self.containers = [container['id'] for container in list(self.db.list_containers())]

    def get_container(self, container):
        if container in self.containers:
            return self.db.get_container_client(container)

class Schema():
    def __init__(self, **kwargs):
        allowed_values = {'text', 'int', 'email', 'password', 'datetime', 'date'}
        self.schema = dict() 
        self.schema.update((k,v) for k, v in kwargs.items() if v in allowed_values)
        self.custom_tests = dict()

    def _date_test(self, date):
        error_message = 'invalid date format'
        try: 
            datetime.strptime(date, "%d/%m/%y")
            return True
        except:
            return error_message

    def _datetime_test(self, datetime):
        error_message = 'invalid datetime format'
        try: 
            datetime.strptime(datetime, "%m/%d/%Y %H:%M:%S")
            return True
        except:
            return error_message

    def _email_test(self, email):
        error_message = 'invalid email format'
        pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        if not(re.fullmatch(pattern, email)):
            return error_message
        else:
            return True

    def _password_requirements_screen(self, password):
        error_message = 'pasword must be minimum eight characters, at least one letter, one number and one special character'
        pattern = r'^(?=.*[A-Za-z])(?=.*\d)(?=.*[@$!%*#?&])[A-Za-z\d@$!%*#?&]{8,}$'
        if not(re.fullmatch(pattern, password)):
            return error_message
        else:
            return True
    
    def _sql_injection_screen(self, text):
        error_message = 'sql injection detected'
        pattern = r"('(''|[^'])*')|(;)|(\b(ALTER|CREATE|DELETE|DROP|EXEC(UTE){0,1}|INSERT( +INTO){0,1}|MERGE|SELECT|UPDATE|UNION( +ALL){0,1})\b)"
        if re.match(pattern, text.upper()):
            return error_message
        else:
            return True
        
    def _int_test(self, int_text):
        error_message = 'not an int'
        try: 
            int(int_text)
            return True
        except:
            return error_message
    
    def _float_test (self, float_text):
        error_message = 'not a float'
        try:
            float(float_text)
            return True
        except:
            return error_message
    
    def set_text_length(self, field, length):
        test = {lambda x: x<=length, f'must not be longer than {str(length)}'}
        try: 
            field in self.schema.keys()
            if field in self.custom_tests.keys():
                self.custom_tests['field'].append(test)
            else:
                self.custom_tests['field']=[test]
        except:
            print('set text length error', self.schema, field, 'field not in schema')

    def add_custom_test(self, field, test, error_message):
        try: 
            field in self.schema.keys()
            if field in self.custom_tests.keys():
                self.custom_tests['field'].append({test, error_message})
            else:
                self.custom_tests['field']=[{test, error_message}]
        except:
            return 'field not in schema'

    def check_kwargs_against_schema(self, comprehensive = True, **kwargs):
        standard_tests = {
            'int':[self._int_test],
            'email':[self._email_test], 
            'password':[self._password_requirements_screen],
            'datetime':[self._datetime_test], 
            'date':[self._date_test],
            'text':[self._sql_injection_screen]
        }
        if comprehensive:
            try: 
                kwargs.keys() == self.schema.keys()
            except:
                return 'kwargs keys don\'t match schema keys'
        else: 
            if not(all([key in self.schema.keys() for key in kwargs.keys()])):
                return 'kwargs keys not in schema keys' 
        for key, value in kwargs.items():
            standard_test_outcomes = [test(value) for test in standard_tests[self.schema[key]]]
            sql_injection_test = [self._sql_injection_screen(value)]
            custom_tests = [True]
            if key in self.custom_tests.keys():
                custom_tests = [test(value) for test in self.custom_tests[key]]
            outcomes = standard_test_outcomes + sql_injection_test + custom_tests
            failures = [outcome for outcome in outcomes if isinstance(outcome, str)]
            if len(failures) > 0:
                failures = ', '.join(failures)
                return f'{key} had errors: {failures}'
        return True
        
class NeoRheoCollection():
    def __init__(self, container, HOST, MASTER_KEY, DATABASE_ID, schema):
        self.client = NeoRheo(HOST, MASTER_KEY, DATABASE_ID)
        self.container = self.client.get_container(container)
        self.schema = schema
        self.pk = self.container.read()['uniqueKeyPolicy']['uniqueKeys'][0]['paths'][0].replace('/','')
        self.partition_key = self.container.read()['partitionKey']['paths'][0].replace('/','')

    def read_id(self):
        return self.container.read_item(item=self.pk, partition_key=self.partition_key)

    def read_by_step(self, step=10):
        step = 50 if step > 50 else step
        return self.container.read_all_items(50)

    def query(self, sql):
        #this is really limited because you can't join across collections
        return list(self.container.query_items(sql,enable_cross_partition_query=True))

    def del_id(self):
        self.container.delete_item(item=self.pk, partition_key=self.partition_key)
    
    def insert(self, **kwargs):
        if self.schema.check_kwargs_against_schema(kwargs):
            # this won't work for bigger applications with lots of writes (an ingestor service is required)
            max_id = int(list(self.query(f'SELECT VALUE MAX(c.{self.pk}) from c'))[0])
            max_id += 1 
            kwargs[self.pk] = max_id
            self.container.upsert_item(body = kwargs)
            return f'new {self.pk} is {max_id}'

    def upsert(self, **kwargs):
        if not self.pk in kwargs.keys():
            return f'error: {self.pk} not in upsert'
        id = kwargs.pop(self.pk)
        if self.schema.check_kwargs_against_schema(kwargs):
            # this won't work for bigger applications with lots of writes (an ingestor service is required)
            kwargs[self.pk] = id
            self.container.upsert_item(body = kwargs)
            return f'{self.pk} is {id}'

