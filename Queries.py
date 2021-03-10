class Queries:

    pay_table_insert = ("""

        SELECT 
            Record_ID ,
            Physician_Profile_ID ,
            Physician_First_Name ,
            Physician_Specialty,
            Total_Amount_of_Payment_USDollars ,
            Nature_of_Payment_or_Transfer_of_Value ,
            Recipient_State 
         
        FROM staging_pay_code_data 

        
    """)

    demographic_table_insert = ("""

        SELECT 
                city_name, 
                state_code,
                state, 
                median_age, 
                male_population, 
                female_population,
                total_population,
                race,
                count
        FROM staging_us_city_data
        
    """)
    
    fact_table_insert = ("""
        SELECT 
            p.Record_ID ,
            p.Physician_Profile_ID ,
            p.Physician_First_Name ,               
            p.Recipient_State,
            p.Physician_Specialty,
             d.city_name,
             d.state_code,
             d.state,
            d.male_population,
        d.female_population,
        d.total_population, 
        d.count
                
        FROM  pay p JOIN demographic d ON p.Recipient_State=d.state_code
  
            """)
