from .cosmos_wrapper.wrapper  import NeoRheoCollection, Schema
import hashlib
from datetime import datetime

class Apps(NeoRheoCollection):
    def __init__(self, HOST, MASTER_KEY, DATABASE_ID):
        self.schema = Schema(navbarName = 'text')
        self.schema.set_text_length('navbarName', 50)
        super().__init__('apps', HOST, MASTER_KEY, DATABASE_ID, self.schema)

    def get_app_list(self, app_ids):
        app_ids = '\', \''.join(app_ids)
        sql = f'SELECT c.id, c.navbarName from c where c.id in (\'{app_ids}\')'
        return self.query(sql)
    
class Permissions(NeoRheoCollection):
    def __init__(self, HOST, MASTER_KEY, DATABASE_ID):
        self.schema = Schema(role_id = 'int', rel = 'text', rel_id='int')
        self.schema.add_custom_test('rel', lambda x: x=='user' or x=='application', 'must be either \'user\' or \'application\'')
        super().__init__('permissions', HOST, MASTER_KEY, DATABASE_ID, self.schema)

    def get_permitted_app_ids_for_user(self, user_id, apps):
        sql = f'SELECT p.role_id from p where p.rel_id=\'{user_id}\' and p.rel=\'user\''
        role_ids = [i['role_id'] for i in self.query(sql)]
        role_ids = '\', \''.join(role_ids)
        sql = f"SELECT p.rel_id from p where p.role_id in ('{role_ids}')"
        app_ids = [i['rel_id'] for i in self.query(sql)]
        return apps.get_app_list(app_ids)

class Roles(NeoRheoCollection):
    def __init__(self, HOST, MASTER_KEY, DATABASE_ID):
        self.schema = Schema(role_name='text')
        super().__init__('roles', HOST, MASTER_KEY, DATABASE_ID, self.schema)

class Users(NeoRheoCollection):
    def __init__(self, HOST, MASTER_KEY, DATABASE_ID):
        self.schema = Schema(user_name= 'text', email='email', password='password')
        self.schema.set_text_length('user_name', 50)
        self.schema.set_text_length('email', 100)
        self.schema.set_text_length('password', 100) 
        super().__init__('users', HOST, MASTER_KEY, DATABASE_ID, self.schema)

    def _get_user_details_from_email(self, email):
        sql = f'SELECT u.user_id, u.user_name, u.email, u.password from u where u.email=\'{email}\''
        user_details = list(self.query(sql))
        return user_details

    def _compare_passwords(self, password, user_details):
        password = password.encode('utf8')
        password = hashlib.blake2b(password).hexdigest()
        return user_details['password'] == password

    def check_user_details_validity(self, email, password, badpassword):
        email = email.lower()
        schema_errors = self.schema.check_kwargs_against_schema(comprehensive= False, email = email, password=password)
        if isinstance(schema_errors, str):
            return (False, schema_errors)
        if not(badpassword.check_brute_force(email)):
            return (False, 'too many failed login attempts for today')
        user_details = self._get_user_details_from_email(email)
        if len(user_details)==0:
            return (False, 'unregistered email')
        elif not(self._compare_passwords(password, user_details)):
            badpassword.count_bad_password(email = user_details[0]['email'])
            return (False,'wrong password')
        return (True, user_details['user_id'])

    def register_user(self, email, password, user_name):
        schema_errors = self.schema.check_kwargs_against_schema(comprehensive= False, email = email, password=password, user_name=user_name)
        if isinstance(schema_errors, str):
            return (False, schema_errors)
        if len(self._get_user_details_from_email(email))==1:
            return (False, 'email already registered')
        password = hashlib.blake2b(password.encode('utf-8')).hexdigest()
        user_id = self.insert(email=email, password=password, user_name=user_name)
        return (True, user_id)
        
class BadPassword(NeoRheoCollection):
    def __init__(self, HOST, MASTER_KEY, DATABASE_ID):
        self.schema = Schema(email='text', date='date')
        self.schema.set_text_length('email', 100)
        super().__init__('badpassword', HOST, MASTER_KEY, DATABASE_ID, self.schema)
        self.date = datetime.utcnow().strftime("%d/%m/%y")
    
    def check_brute_force(self, email):
        sql = f"SELECT b.id from b where b.email='{email}' and b.date = '{self.date}'"
        attempt_count = len(self.query(sql))
        return attempt_count<4
            
    def count_bad_attempt(self, email):
        self.insert(email=email, date=self.date)

class TokenService(NeoRheoCollection):
    def __init__(self, HOST, MASTER_KEY, DATABASE_ID):
        self.schema = Schema(email = 'text', token= 'text', datetime='datetime', date='date')
        self.schema.set_text_length('email', 100)
        self.schema.set_text_length('token', 128)
        super().__init__('tokens', HOST, MASTER_KEY, DATABASE_ID, self.schema)
    
    def generate_token(self, user_details):
        return 'this_is_a_token'

    def validate_token(self, token, lifespan):
        init_token_time = self.query(f'Select datetime from tokens where token = {token}')
        try:
            time_diff = (datetime.utcnow() - datetime.strptime(init_token_time[0], '%m/%d/%Y, %H:%M:%S')).total_seconds()/60
            return time_diff < lifespan
        except:
            return False
