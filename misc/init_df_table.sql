CREATE TABLE IF NOT EXISTS `FuncDep`(
    `table` VARCHAR NOT NULL,
    `lhs` VARCHAR NOT NULL,
    `rhs` VARCHAR NOT NULL,

    CONSTRAINT `FuncDep_pk` PRIMARY KEY (`table`, `lhs`, `rhs`)
);