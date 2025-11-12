CREATE DATABASE IF NOT EXISTS bankdb CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE bankdb;

CREATE TABLE IF NOT EXISTS bank_customers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    age INT NOT NULL,
    balance DOUBLE,
    housing TINYINT,
    loan TINYINT,
    campaign INT
) ENGINE=InnoDB;
