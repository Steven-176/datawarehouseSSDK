-- Table des dimensions
CREATE TABLE IF NOT EXISTS Departement (
    num_departement VARCHAR(255) NOT NULL,
    nom_departement VARCHAR(255) NOT NULL,
    nom_region VARCHAR(255) NOT NULL,
    PRIMARY KEY (num_departement)
);

CREATE TABLE IF NOT EXISTS Passage (
    id_passage INT NOT NULL,
    nombre_passage_corona INT,
    nombre_passage_total INT,
    nombre_passage_corona_h INT,
    nombre_passage_corona_f INT,
    nombre_passage_total_h INT,
    nombre_passage_total_f INT,
    PRIMARY KEY (id_passage)
);

CREATE TABLE IF NOT EXISTS Hospitalisation (
    id_hospitalisation INT NOT NULL,
    nombre_hospitalisation_corona INT,
    nombre_hospitalisation_corona_h INT,
    nombre_hospitalisation_corona_f INT,
    PRIMARY KEY (id_hospitalisation)
);

CREATE TABLE IF NOT EXISTS Age (
    id_age INT NOT NULL,
    tranche_age VARCHAR(255) NOT NULL,
    PRIMARY KEY (id_age)
);

-- Table des faits
CREATE TABLE IF NOT EXISTS Urgence_covid (
    -- id_urgence_covid INT NOT NULL,
    -- id_hospitalisation INT FOREIGN KEY REFERENCES Hospitalisation(id_hospitalisation),
    -- id_passage INT FOREIGN KEY REFERENCES Passage(id_passage),
    -- id_age INT FOREIGN KEY REFERENCES Age(id_age),
    -- num_departement INT FOREIGN KEY REFERENCES Departement(num_departement),
    -- PRIMARY KEY (id_urgence_covid)
    id_urgence_covid INT NOT NULL,
    id_hospitalisation INT,
    id_passage INT,
    id_age INT,
    num_departement VARCHAR(255),
    PRIMARY KEY (id_urgence_covid),
    FOREIGN KEY (id_hospitalisation) REFERENCES Hospitalisation(id_hospitalisation),
    FOREIGN KEY (id_passage) REFERENCES Passage(id_passage),
    FOREIGN KEY (id_age) REFERENCES Age(id_age),
    FOREIGN KEY (num_departement) REFERENCES Departement(num_departement)
);

-- INSERT INTO Age VALUES (0, "tous âges");
-- INSERT INTO Age VALUES (1, "0-4 ans");
-- INSERT INTO Age VALUES (2, "5-14 ans");
-- INSERT INTO Age VALUES (3, "15-44 ans");
-- INSERT INTO Age VALUES (4, "45-64 ans");
-- INSERT INTO Age VALUES (5, "65-74 ans");
-- INSERT INTO Age VALUES (6, "75 ans et plus");

INSERT INTO Age (id_age, tranche_age) VALUES (0, 'tous âges') ON CONFLICT (id_age) DO NOTHING;
INSERT INTO Age (id_age, tranche_age) VALUES (1, '0-4 ans') ON CONFLICT (id_age) DO NOTHING;
INSERT INTO Age (id_age, tranche_age) VALUES (2, '5-14 ans') ON CONFLICT (id_age) DO NOTHING;
INSERT INTO Age (id_age, tranche_age) VALUES (3, '15-44 ans') ON CONFLICT (id_age) DO NOTHING;
INSERT INTO Age (id_age, tranche_age) VALUES (4, '45-64 ans') ON CONFLICT (id_age) DO NOTHING;
INSERT INTO Age (id_age, tranche_age) VALUES (5, '65-74 ans') ON CONFLICT (id_age) DO NOTHING;
INSERT INTO Age (id_age, tranche_age) VALUES (6, '75 ans et plus') ON CONFLICT (id_age) DO NOTHING;
