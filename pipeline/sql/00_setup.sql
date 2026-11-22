-- Catalogue du projet
CREATE CATALOG IF NOT EXISTS tpe_fraud;

-- Zone d'arrivée des fichiers bruts (Volumes)
CREATE SCHEMA IF NOT EXISTS tpe_fraud.landing;
CREATE VOLUME IF NOT EXISTS tpe_fraud.landing.iot;    -- fichiers du flux TPE
CREATE VOLUME IF NOT EXISTS tpe_fraud.landing.bank;   -- fichiers CSV de la banque
CREATE VOLUME IF NOT EXISTS tpe_fraud.landing.checkpoints; -- Auto Loader’s memory

-- Zones du Lakehouse (tables Delta créées par les notebooks)
CREATE SCHEMA IF NOT EXISTS tpe_fraud.bronze;
CREATE SCHEMA IF NOT EXISTS tpe_fraud.silver;
CREATE SCHEMA IF NOT EXISTS tpe_fraud.gold;
CREATE SCHEMA IF NOT EXISTS tpe_fraud.quarantaine;

-- Vérification
SHOW SCHEMAS IN tpe_fraud;
SHOW VOLUMES IN tpe_fraud.landing;