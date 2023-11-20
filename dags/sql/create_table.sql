CREATE TABLE IF NOT EXISTS departement (
    num_departement INT NOT NULL,
    nom_departement VARCHAR(255) NOT NULL,
    nom_region VARCHAR(255) NOT NULL,
    PRIMARY KEY (num_departement)
);

CREATE TABLE IF NOT EXISTS passage (
    id_passage INT NOT NULL,
    nombre_passage_corona INT,
    nombre_passage_total INT,
    nombre_passage_corona_h INT,
    nombre_passage_corona_f INT,
    nombre_passage_total_h INT,
    nombre_passage_total_f INT,
    PRIMARY KEY (id_passage)
);

CREATE TABLE IF NOT EXISTS hospitalisation (
    id_hospitalisation INT NOT NULL,
    nombre_hospitalisation_corona INT,
    nombre_hospitalisation_corona_h INT,
    nombre_hospitalisation_corona_f INT,
    PRIMARY KEY (id_hospitalisation)
);

-- Table Principale
-- CREATE TABLE IF NOT EXISTS urgence_covid (
--     num_departement INT NOT NULL,
--     nom_departement VARCHAR(255) NOT NULL,
--     nom_region VARCHAR(255) NOT NULL,
--     PRIMARY KEY (num_departement)
-- );