CREATE TABLE staging_us_city_data (
        city_name                   VARCHAR,
        state                       VARCHAR,
        median_age                  FLOAT,
        male_population             NUMERIC,
        female_population           NUMERIC,
        total_population            NUMERIC,
        number_of_veterans          NUMERIC,
        foreign_born                NUMERIC,
        average_household_size      DECIMAL(6,3),
        state_code                  VARCHAR,
        race                        VARCHAR,
        count                       NUMERIC
);

CREATE TABLE  staging_pay_code_data (
    Record_ID NUMERIC,
    Physician_Profile_ID NUMERIC,
    Physician_First_Name VARCHAR,
    Physician_Specialty VARCHAR,
    Total_Amount_of_Payment_USDollars NUMERIC,
    Nature_of_Payment_or_Transfer_of_Value VARCHAR,
    Recipient_State VARCHAR
 );


CREATE TABLE IF NOT EXISTS  pay (
    Record_ID BIGINT NOT NULL,
    Physician_Profile_ID FLOAT NOT NULL,
    Physician_First_Name VARCHAR(500) ,
    Physician_Specialty VARCHAR(500),
    Total_Amount_of_Payment_USDollars BIGINT,
    Nature_of_Payment_or_Transfer_of_Value VARCHAR(10000) ,
    Recipient_State VARCHAR(500) NOT NULL,
    CONSTRAINT pay_pk PRIMARY KEY (Recipient_State)
);

CREATE TABLE IF NOT EXISTS demographic (
    city_name VARCHAR(500) NOT NULL,
    state_code VARCHAR(50) NOT NULL,
    state VARCHAR(50) NOT NULL,
    median_age FLOAT,
    male_population BIGINT,
    female_population BIGINT,
    total_population BIGINT,
    race  VARCHAR(50),
    count  BIGINT,
    CONSTRAINT demographic_pk PRIMARY KEY (city_name, state_code)
);


CREATE TABLE IF NOT EXISTS fact_table (
    Record_ID BIGINT NOT NULL,
    Physician_Profile_ID FLOAT NOT NULL,
    Physician_First_Name VARCHAR(500) NOT NULL,
    Physician_Specialty VARCHAR(500) NOT NULL,
    Recipient_State text NOT NULL,
    city_name VARCHAR(500) NOT NULL,
    state_code text NOT NULL,
    state text NOT NULL,
    male_population BIGINT NOT NULL ,
    female_population BIGINT NOT NULL,
    total_population BIGINT NOT NULL,
    count  BIGINT NOT NULL,
    CONSTRAINT fact_pk PRIMARY KEY (Recipient_State,city_name, state_code)  );
    