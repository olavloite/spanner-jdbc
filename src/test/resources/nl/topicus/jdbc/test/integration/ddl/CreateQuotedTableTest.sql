CREATE TABLE `TEST_QUOTED`
(
	`Id` INT64 NOT NULL,
	`UUID` BYTES(16) NOT NULL,
	`active` BOOL,
	`Amount` FLOAT64,
	`Description` STRING(100),
	`Created_Date` DATE,
	`Last_Updated` TIMESTAMP
) PRIMARY KEY (`Id`)
