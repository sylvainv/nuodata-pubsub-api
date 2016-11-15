--/**
-- * Manati PostgreSQL REST API
-- * Copyright (C) 2016 Sylvain Verly
-- *
-- * This program is free software: you can redistribute it and/or modify
-- * it under the terms of the GNU Affero General Public License as
-- * published by the Free Software Foundation, either version 3 of the
-- * License, or any later version.
-- *
-- * This program is distributed in the hope that it will be useful,
-- * but WITHOUT ANY WARRANTY; without even the implied warranty of
-- * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- * GNU Affero General Public License for more details.
--
-- * You should have received a copy of the GNU Affero General Public License
-- * along with this program.  If not, see <http://www.gnu.org/licenses/>.
-- */
BEGIN;

DROP TABLE IF EXISTS uuid_data CASCADE;
DROP TABLE IF EXISTS number_data CASCADE;
DROP TABLE IF EXISTS string_data CASCADE;
DROP TABLE IF EXISTS time_data CASCADE;
DROP TABLE IF EXISTS misc_data CASCADE;
DROP TABLE IF EXISTS ip_data CASCADE;
DROP TABLE IF EXISTS json_data CASCADE;
DROP TABLE IF EXISTS range_data CASCADE;
DROP TABLE IF EXISTS gis_data CASCADE;
DROP TYPE IF EXISTS mood CASCADE;

COMMIT;