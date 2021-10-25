from .collections.auth_collections import Apps, Permissions, Roles, TokenService, Users, BadPassword
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))

class UserAuthentication():
    def __init__(self, HOST, MASTER_KEY, DATABASE_ID):
        self.apps = Apps(HOST, MASTER_KEY, DATABASE_ID)
        self.permissions = Permissions(HOST, MASTER_KEY, DATABASE_ID)
        self.roles = Roles(HOST, MASTER_KEY, DATABASE_ID)
        self.users = Users(HOST, MASTER_KEY, DATABASE_ID)
        self.bad_password = BadPassword(HOST, MASTER_KEY, DATABASE_ID)
        self.token_service = TokenService(HOST, MASTER_KEY, DATABASE_ID)

    def validate_user(self, user_details):
        email = user_details['email']
        password = user_details['password']
        result = self.users.check_user_details_validity(email, password, self.bad_password)
        if result[0]:
            token = self.token_service.generate_token({'email': email, 'password': password})
            return {'token':token, 'user_id':result[1]}
        else:
            return {'error':result[1]}
    
    def register_user(self, user_details):
        email = user_details['email']
        password = user_details['password']
        user_name = user_details['user_name']
        result = self.users.register_user(email, password, user_name)
        if result[0]:
            token = self.token_service.generate_token({'email': email, 'password': password})
            return {'token':token, 'user_id':result[1]}
        else:
            return {'error':result[1]}
        
    def get_user_apps(self, id):
        return {'apps': self.permissions.get_permitted_app_ids_for_user(id,self.apps)}
    
    def validate_token(self, token):
        if self.token_service.validate_token(token):
            return {'token_status':1}
        else:
            return {'token_status':0}
        

