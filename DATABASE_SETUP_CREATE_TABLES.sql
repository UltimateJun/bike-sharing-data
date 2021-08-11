--
-- Table structure for table `bike`
--

DROP TABLE IF EXISTS `bike`;
CREATE TABLE `bike` (
  `bike_id` varchar(40) NOT NULL,
  `provider` varchar(20) NOT NULL,
  PRIMARY KEY (`bike_id`)
) ENGINE=InnoDB;

--
-- Table structure for table `station`
--
DROP TABLE IF EXISTS `station`;
CREATE TABLE `station` (
  `station_id` varchar(40) NOT NULL,
  `coordinates` point NOT NULL SRID 4326,
  `name` varchar(200) DEFAULT NULL,
  `provider` varchar(20) NOT NULL,
  `capacity` smallint DEFAULT NULL,
  PRIMARY KEY (`station_id`),
  SPATIAL INDEX `coordinates_index` (`coordinates`)
) ENGINE=InnoDB;

--
-- Table structure for table `bike_last_status`
--
DROP TABLE IF EXISTS `bike_last_status`;
CREATE TABLE `bike_last_status` (
  `bike_id` varchar(40) NOT NULL,
  `coordinates` point NOT NULL SRID 4326,
  `station_id` varchar(40) DEFAULT NULL,
  `since` datetime NOT NULL,
  PRIMARY KEY (`bike_id`),
  SPATIAL INDEX `coordinates_index` (`coordinates`),
  CONSTRAINT `station_fk` FOREIGN KEY (`station_id`) REFERENCES `station` (`station_id`),
  CONSTRAINT `bike_fk` FOREIGN KEY (`bike_id`) REFERENCES `bike` (`bike_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB;

--
-- Table structure for table `bike_ride`
--
DROP TABLE IF EXISTS `bike_ride`;
CREATE TABLE `bike_ride` (
  `bike_ride_id` int NOT NULL AUTO_INCREMENT,
  `bike_id` varchar(40) DEFAULT NULL,
  `start_coordinates` point NOT NULL SRID 4326,
  `end_coordinates` point NOT NULL SRID 4326,
  `start_station_id` varchar(40) DEFAULT NULL,
  `end_station_id` varchar(40) DEFAULT NULL,
  `since` datetime NOT NULL,
  `until` datetime NOT NULL,
  PRIMARY KEY (`bike_ride_id`),
  UNIQUE KEY `bike_ride_id_UNIQUE` (`bike_ride_id`),
  UNIQUE KEY `unique_rides` (`bike_id`,`since`) COMMENT 'only one ride with the combination bike_id + since allowed',
  INDEX `bike_id_fk_idx` (`bike_id`),
  INDEX `bike_start_station_fk_idx` (`start_station_id`),
  INDEX `bike_end_station_fk_idx` (`end_station_id`),
  SPATIAL INDEX `start_index` (`start_coordinates`),
  SPATIAL INDEX `end_index` (`end_coordinates`),
  INDEX `since_index` (`since`) USING BTREE,
  INDEX `until_index` (`until`) USING BTREE,
  CONSTRAINT `bike_end_station_fk` FOREIGN KEY (`end_station_id`) REFERENCES `station` (`station_id`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `bike_id_fk` FOREIGN KEY (`bike_id`) REFERENCES `bike` (`bike_id`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `bike_start_station_fk` FOREIGN KEY (`start_station_id`) REFERENCES `station` (`station_id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE=InnoDB;

--
-- Table structure for table `ride_analysis`
--
DROP TABLE IF EXISTS `ride_analysis`;
CREATE TABLE `ride_analysis` (
  `bike_ride_id` int NOT NULL,
  `duration_min` int DEFAULT NULL,
  `distance_meters` decimal(10,2) DEFAULT NULL,
  `sameStations` tinyint DEFAULT NULL,
  `suspicious` tinyint DEFAULT NULL,
  PRIMARY KEY (`bike_ride_id`),
  UNIQUE KEY `bike_ride_id_UNIQUE` (`bike_ride_id`),
  CONSTRAINT `ride_fk` FOREIGN KEY (`bike_ride_id`) REFERENCES `bike_ride` (`bike_ride_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB;

--
-- Table structure for table `district`
--
DROP TABLE IF EXISTS `district`;
CREATE TABLE `district` (
  `district_id` int NOT NULL,
  `name` varchar(60) NOT NULL,
  `area` polygon NOT NULL SRID 4326,
  PRIMARY KEY (`district_id`),
  SPATIAL INDEX `area_index` (`area`)
) ENGINE=InnoDB;

--
-- Table structure for table `exception`
--
DROP TABLE IF EXISTS `exception`;
CREATE TABLE `exception` (
  `exception_id` int NOT NULL AUTO_INCREMENT,
  `provider` varchar(20) DEFAULT NULL,
  `error_type` varchar(30) DEFAULT NULL,
  `time` datetime DEFAULT NULL,
  PRIMARY KEY (`exception_id`),
  INDEX `time_index` (`time`) USING BTREE
) ENGINE=InnoDB;