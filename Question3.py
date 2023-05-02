from credentials import question


query = """
SELECT "Main_port_name", 
		"Wpi_country_code", 
		"Latitude_degrees", 
		"Longitude_degrees",
        point("Longitude_degrees", "Latitude_degrees") <-> point(-38.706256, 32.610982) as distance

FROM "WPI_data"
WHERE "Supplies_provisions" = 'Y' AND "Supplies_water" = 'Y' AND "Supplies_fuel_oil" = 'Y' AND "Supplies_diesel_oil" = 'Y'
ORDER BY point("Longitude_degrees", "Latitude_degrees") <-> point(-38.706256, 32.610982) ASC
LIMIT 1;
        """

question(query,3)