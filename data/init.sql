
CREATE DATABASE IF NOT EXISTS program_analytics;

use program_analytics;

CREATE TABLE IF NOT EXISTS programdata (
	id INT(11) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	agency VARCHAR(260) NULL,
	missionarea VARCHAR(260) NULL,
	division VARCHAR(260) NULL,
  program VARCHAR(260) NULL,
  programcategory VARCHAR(260) NULL,
  url TEXT NULL,
  domain VARCHAR(260) NULL,
  description TEXT NULL,
  programtype VARCHAR(260) NULL,
  cost VARCHAR(260) NULL,
  reviewed_by VARCHAR(260) NULL,
  visits INT(11) NULL,
  pageviews INT(11) NULL,
  date TIMESTAMP
);
