from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField
from wtforms.validators import DataRequired, EqualTo, Length


class LoginForm(FlaskForm):
	username = StringField('Username', validators=[DataRequired()])
	password = PasswordField('Password', validators=[DataRequired()])
	submit = SubmitField('Sign In')


class ChangePWForm(FlaskForm):
	old_password = PasswordField('Old Password', validators=[DataRequired()])
	new_password = PasswordField('New Password', validators=[
		DataRequired(),
		Length(min=6, max=35)])
	confirm_password = PasswordField('Confirm New Password', validators=[
		DataRequired(),
		EqualTo('new_password', message='Passwords do not match'),
		Length(min=6, max=35)])
	submit = SubmitField('Change Password')


class AddUserForm(FlaskForm):
	username = StringField('Add Username', validators=[DataRequired()])
	first_name = StringField('First Name')
	last_name = StringField('Last Name')
	submit = SubmitField('Add User')


class PermissionChangeForm(FlaskForm):
	password = PasswordField('Password', validators=[DataRequired()])
	submit = SubmitField('Submit User Changes')
