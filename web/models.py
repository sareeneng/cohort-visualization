import logging
from web import db, login
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import UserMixin

@login.user_loader
def load_user(id):
	return User.query.get(int(id))


class User(UserMixin, db.Model):
	id = db.Column(db.Integer, primary_key=True)
	username = db.Column(db.String(64), index=True, unique=True)
	password_hash = db.Column(db.String(120))
	first_name = db.Column(db.String(64), index=True)
	last_name = db.Column(db.String(64), index=True)

	def __init__(self, **kwargs):
		super(User, self).__init__(**kwargs)
	
	def assign_group(self, group_name='Basic'):
		group_id = db.session.query(Group).filter(Group.group_name == group_name).first().id
		user_group_rel = UserGroups(user_id=self.id, group_id=group_id)
		db.session.add(user_group_rel)
		db.session.commit()

	def get_roles(self):
		# Returns list of roles
		db_results = db.session.query(UserGroups, Group).join(Group).filter(UserGroups.user_id == self.id).all()
		group_names = [x.Group.group_name for x in db_results]
		logging.debug(f'User {self.username} has roles: {group_names}')
		return group_names

	def set_password(self, password):
		self.password_hash = generate_password_hash(password)
	
	def check_password(self, password):
		return check_password_hash(self.password_hash, password)

	@property
	def full_name(self):
		return f'{self.last_name}, {self.first_name}'


class Group(db.Model):
	id = db.Column(db.Integer, primary_key=True)
	group_name = db.Column(db.String())


class UserGroups(db.Model):
	id = db.Column(db.Integer, primary_key=True)
	user_id = db.Column(db.Integer, db.ForeignKey(User.id))
	group_id = db.Column(db.Integer, db.ForeignKey(Group.id))
