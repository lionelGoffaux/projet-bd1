CREATE TABLE IF NOT EXISTS `TRIPS`(
    `Date` VARCHAR NOT NULL,
    `Number_Plate` VARCHAR(7) NOT NULL,
    `Driver` VARCHAR NOT NULL,
    `Destination` VARCHAR NOT NULL,
    `Departure_Time` DATETIME NOT NULL,

    CONSTRAINT `TRIPS_pk` PRIMARY KEY (`Date`, `Driver`, `Departure_Time`)
);

CREATE TABLE IF NOT EXISTS `BUSES`(
    `Number_Plate` VARCHAR(7) NOT NULL,
    `Chassis` VARCHAR NOT NULL,
    `Make` VARCHAR NOT NULL,
    `Mileage` INTEGER NOT NULL,

    CONSTRAINT `BUSES_pk` PRIMARY KEY (`Number_Plate`)
);

CREATE TABLE IF NOT EXISTS `DESTINATIONS`(
    `Name` VARCHAR NOT NULL,

    CONSTRAINT `DESTINATIONS_pk` PRIMARY KEY (`Name`)
);

INSERT INTO `TRIPS` VALUES  
    ("15/10/2001",   "DDT 123",    "John",   "Antwerp Zoo",        "09.00"),
    ("15/10/2001",   "LPG 234",    "Tim",    "Ostende Beach",      "08.00"),
    ("16/10/2001",   "DDT 123",    "Tim",    "Dinant Citadel",     "10.00"),
    ("17/10/2001",   "LPG 234",    "John",   "Antwerp Zoo",        "08.15"),
    ("17/10/2001",   "DDT 123",    "Tim",    "Antwerp Zoo",        "08.15"),
    ("18/10/2001",   "DDT 123",    "Tim",    "Brussels Atomium",   "09.20");

INSERT INTO `BUSES` VALUES
    ("DDT 123",    "XGUR6775",   "Renault",    212342),
    ("DDT 456",    "XGUR6775",   "Mercedes",    212350),
    ("LPG 234",    "ZXRY9823",   "Mercedes",   321734),
    ("RAM 221",    "XXZZ7345",   "Renault",     10000);