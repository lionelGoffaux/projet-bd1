CREATE TABLE IF NOT EXISTS `TRIPS`(
    `Date` DATE NOT NULL,
    `Number_Plate` VARCHAR(7) NOT NULL,
    `Driver` VARCHAR NOT NULL,
    `Destination` VARCHAR NOT NULL,
    `Departure_Time` DATETIME NOT NULL,

    CONSTRAINT `TRIPS_pk` PRIMARY KEY (`Date`, `Driver`, `Departure_Time`),
    CONSTRAINT `Number_Plate_fk` FOREIGN KEY (`Number_Plate`) REFERENCES BUSES(`Number_Plate`),
    CONSTRAINT `Destination_fk` FOREIGN KEY (`Destination`) REFERENCES DESTINATIONS(`Name`)
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