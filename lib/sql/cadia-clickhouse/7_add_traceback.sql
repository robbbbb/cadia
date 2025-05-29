-- Adds field for traceback in the errors table

ALTER TABLE cadia.errors ADD COLUMN traceback String;
