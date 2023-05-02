from credentials import question


query = """
SELECT "Main_port_name", "Wpi_country_code", 
		"Latitude_degrees", "Longitude_degrees",
         6371000 * acos(
         cos(radians("Latitude_degrees")) *
         cos(radians((SELECT "Latitude_degrees" FROM "WPI_data" WHERE "Main_port_name" = 'JURONG ISLAND' AND "Wpi_country_code" = 'SG'))) *
         cos(radians((SELECT "Longitude_degrees" FROM "WPI_data" WHERE "Main_port_name" = 'JURONG ISLAND' AND "Wpi_country_code" = 'SG')) - radians("Longitude_degrees")) +
         sin(radians("Latitude_degrees")) *
         sin(radians((SELECT "Latitude_degrees" FROM "WPI_data" WHERE "Main_port_name" = 'JURONG ISLAND' AND "Wpi_country_code" = 'SG')))
       ) AS distance
FROM "WPI_data"
WHERE "Main_port_name" != 'JURONG ISLAND' AND "Wpi_country_code" = 'SG'
ORDER BY distance ASC
LIMIT 5;
        """

question(query,1)




