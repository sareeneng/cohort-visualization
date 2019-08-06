from flask_wtf import FlaskForm
from wtforms import SelectField, SubmitField, StringField

class ConfigForm(FlaskForm):
	dataset = SelectField('Dataset')
	table = SelectField('Table')
	file_col_name = StringField('')