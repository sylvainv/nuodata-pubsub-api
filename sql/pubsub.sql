BEGIN;

CREATE SCHEMA IF NOT EXISTS nuodata_pubsub;

-- Mark table for watch creation trigger
CREATE OR REPLACE FUNCTION nuodata_pubsub.watch(target text) RETURNS void AS $$
BEGIN
  BEGIN
    EXECUTE 'CREATE TRIGGER notify_on_change AFTER UPDATE OR DELETE OR INSERT on ' || quote_ident(target) || ' FOR EACH ROW EXECUTE PROCEDURE nuodata_pubsub.notify_on_change()';
    EXCEPTION
      WHEN duplicate_object THEN
        NULL;
  END;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION nuodata_pubsub.unwatch(target text) RETURNS void AS $$
BEGIN
  EXECUTE 'DROP TRIGGER IF EXISTS notify_on_change ON ' || quote_ident(target);
END;
$$ LANGUAGE plpgsql;

-- Timestamps update trigger
CREATE OR REPLACE FUNCTION nuodata_pubsub.notify_on_change() RETURNS trigger AS $$
DECLARE _data json;
BEGIN
  IF (TG_OP = 'DELETE') THEN
      _data = row_to_json(OLD);
  ELSE
      _data = row_to_json(NEW);
  END IF;

  PERFORM pg_notify(
    lower(format('%s__%s__%s', TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME)),
    _data::text
  );
  -- notify of notify as well
  PERFORM pg_notify(
    lower(format('change__%s__%s', TG_TABLE_SCHEMA, TG_TABLE_NAME)),
    _data::text
  );

  RETURN NULL; -- result is ignored since this is an AFTER trigger
END;
$$ LANGUAGE plpgsql;

-- build the channel name to listen to
CREATE OR REPLACE FUNCTION nuodata_pubsub._build_channel(target text, type text) RETURNS text AS $$
DECLARE _schema_name text;
DECLARE _target text;
DECLARE _channel text;
DECLARE _has_privilege boolean;
BEGIN
  -- regclass casting will change a schema qualified name into the table name, if it doesn't exists it will throw an error
  _target := target::regclass::text;
  -- check if the relation exist
  -- if the table is not found the ::regclass cast will raise a table not found error
  SELECT schemaname INTO _schema_name FROM pg_tables WHERE schemaname = ANY (CURRENT_SCHEMAS(false)) AND tablename = _target;

  -- check table privilege
  SELECT has_table_privilege(target, 'select') INTO _has_privilege;
  IF NOT _has_privilege THEN
    RAISE prohibited_sql_statement_attempted USING message = 'You do not have access to read this table';
  END IF;

  -- format channel name
  _channel := format('%s__%s__%s', lower(type), _schema_name, _target);

  return _channel;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- listen for changes on the table
CREATE OR REPLACE FUNCTION nuodata_pubsub.subscribe(target text, type text) RETURNS text AS $$
DECLARE _channel text;
BEGIN
  -- format channel name
  SELECT nuodata_pubsub._build_channel(target, type) INTO _channel;
  EXECUTE format('LISTEN %s', _channel);
  return _channel;
END;
$$ LANGUAGE plpgsql;

-- stop listening for changes on the table
CREATE OR REPLACE FUNCTION nuodata_pubsub.unsubscribe(target text, type text) RETURNS text AS $$
DECLARE _channel text;
BEGIN
  -- format channel name
  SELECT nuodata_pubsub._build_channel(target, type) INTO _channel;
  EXECUTE format('UNLISTEN %s', _channel);
  return _channel;
END;
$$ LANGUAGE plpgsql;

COMMIT;