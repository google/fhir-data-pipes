-- Copyright 2020 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- This is to create a database for Atom Feed Client of OpenMRS.
-- To use this, run: mysql --user=[USER] --password=[PASSWORD] < create_db.sql
-- Or use this for a password prompt: mysql --user=[USER] -p < create_db.sql

CREATE DATABASE IF NOT EXISTS `atomfeed_client`;
USE `atomfeed_client`;

CREATE TABLE `failed_events` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `feed_uri` varchar(255) DEFAULT NULL,
  `failed_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `error_message` longtext DEFAULT NULL,
  `event_id` varchar(255) DEFAULT NULL,
  `event_content` longtext DEFAULT NULL,
  `tags` varchar(255) DEFAULT NULL,
  `title` varchar(255) DEFAULT NULL,
  `retries` int(11) NOT NULL DEFAULT 0,
  `error_hash_code` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`)
);

CREATE TABLE `markers` (
  `feed_uri` varchar(255) NOT NULL,
  `last_read_entry_id` varchar(255) DEFAULT NULL,
  `feed_uri_for_last_read_entry` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`feed_uri`)
);

