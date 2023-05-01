-- Copyright (c) 2015 YCSB contributors. All rights reserved.
--
-- Licensed under the Apache License, Version 2.0 (the "License"); you
-- may not use this file except in compliance with the License. You
-- may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
-- implied. See the License for the specific language governing
-- permissions and limitations under the License. See accompanying
-- LICENSE file.

-- Creates a Table.

-- Drop the table if it exists;
DROP TABLE IF EXISTS usermetric;

-- Create the usermetric table with 10 fields and 1 double value and Timestamp as key
CREATE TABLE usermetric(
  ID INT IDENTITY(1,1) PRIMARY KEY,
  YCSB_KEY TIMESTAMP,
  VALUE DOUBLE PRECISION,
  TAG0 VARCHAR, TAG1 VARCHAR,
  TAG2 VARCHAR, TAG3 VARCHAR,
  TAG4 VARCHAR, TAG5 VARCHAR,
  TAG6 VARCHAR, TAG7 VARCHAR,
  TAG8 VARCHAR, TAG9 VARCHAR);
