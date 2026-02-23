/*
===============================================================================
Scalar Function: Clean and Normalize City Names (Bronze -> Silver)
===============================================================================
Script Purpose:
    This function normalizes city name strings coming from the bronze layer.
    It performs the following actions:
    - Removes or replaces accented characters (e.g., 'à' -> 'a', 'ç' -> 'c').
    - Strips unwanted characters (e.g., '.', '*', ',', ';', '%').
    - Corrects known misspellings and data entry inconsistencies.
    - Expands abbreviations to full city names (e.g., 'sp' -> 'sao paulo').

Parameters:
    @input NVARCHAR(50):
        The raw city name string to be cleaned. 

Returns:
    NVARCHAR(50):
        The cleaned and normalized city name string.

Usage Example:
    SELECT clean_names_fn(geolocation_city)
    FROM bronze.geolocation;
===============================================================================
*/

CREATE OR ALTER FUNCTION silver.clean_names_fn
(
    @input NVARCHAR(50)
)
RETURNS NVARCHAR(50)
AS
BEGIN
    DECLARE @result NVARCHAR(50)

    SET @result = 
		TRIM(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(
		REPLACE(@input, 
			'à', 'a'),
			'á', 'a'),
			'â', 'a'),
			'ã', 'a'),
			'è', 'e'),
			'é', 'e'),
			'ê', 'e'),
			'ì', 'i'),
			'í', 'i'),
			'ò', 'o'),
			'ó', 'o'),
			'ô', 'o'),
			'õ', 'o'),
			'ù', 'u'),
			'ú', 'u'),
			'ü', 'u'),
			'ç', 'c'),
			'*', ''),
			'.', ''),
			'`', ''),
			'´', ''),
			'-', ' '),
			'  ', ''),
			'%', ''),
			',', ''),
			'4o', 'quarto'),
			'4º', 'quarto'),
			'³', ''),
			'aquidaban', 'aquidaba'),
			'(saquarema) distrito', ''),
			';', ''),
			'&', ''),
			'florianoacutepolis', 'florianopolis'),
			'guarulhos sp', 'guarulhos'),
			'(mucuri)', ''),
			'doeste', 'd''oeste'),
			'do oeste', 'd''oeste'),
			'd26aposboeste', 'd''oeste'),
			'gurarapes', 'guararapes'),
			'd ', 'd'''),
			'd''oeste mg', 'd''oeste'),
			'(cabreuva)', ''),
			'dagua', 'd''agua'),
			'(camacari) distrito', ''),
			'(camacari)', ''),
			'(fundao)', ''),
			'(itatiaia)', ''),
			'(manhuacu)', ''),
			'(cabreuva)', ''),
			'(saquarema)', ''),
			'(igaratinga)', ''),
			'(barra do pirai)', ''),
			'(mucuri)', ''),
			'muquem do sao francisco', 'muquem de sao francisco'),
			'(cabo frio)', ''),
			'rio de janeiro brasil', ''),
			'xangrila', 'xangri la'),
			'santssima', 'santissima'),
			'sant''ana', 'santana'),
			'aelgre', 'alegre'),
			'bahia brasil', ''),
			'(porto seguro)', ''),
			'/ minas gerais', ''),
			'/ sao paulo', ''),
			'/sao paulo', ''),
			'/ sp', ''),
			'/sp', ''),
			'/ es', ''),
			'/pr', ''),
			'/ rio de janeiro', ''),
			'\rio de janeiro', '')
		)

	SET @result =
	CASE @result 
		WHEN 'rj' THEN 'rio de janeiro'
		WHEN 'sp' THEN 'sao paulo'
		WHEN '04482255' THEN NULL
		WHEN 'belford''roxo' THEN 'belford roxo'
		WHEN 'novo hamburgo rio grande do sul brasil' THEN 'novo hamburgo'
		WHEN 's jose do rio preto' THEN 'sao jose do rio preto'
		WHEN 'sao jose do rio pret' THEN 'sao jose do rio preto'
		WHEN 'sao paulo sp' THEN 'sao paulo'
		WHEN 'sao paulop' THEN 'sao paulo'
		WHEN 'sao pauo' THEN 'sao paulo'
		WHEN 'sao paluo' THEN 'sao paulo'
		WHEN 'saopaulo' THEN 'sao paulo'
		WHEN 'vendas@creditpartscombr' THEN NULL
		WHEN 'angra dos reis rj' THEN 'angra dos reis'
		WHEN 'balenario camboriu' THEN 'balneario camboriu'
		WHEN 'brasilia df' THEN 'brasilia'
		WHEN 'aguas claras df' THEN 'aguas claras'
		WHEN 'ferraz devasconcelos' THEN 'ferraz de vasconcelos'
		WHEN 'mand''aguacu' THEN 'mandaguacu'
		WHEN 'mand''aguari' THEN 'mandaguari'
		WHEN 'sbc' THEN NULL
		WHEN 'scao jose do rio pardo' THEN 'sao jose do rio pardo'
		WHEN 'piumhii' THEN 'piumhi'
		WHEN 'andira pr' THEN 'andira'
		WHEN 'lages sc' THEN 'lages'
		ELSE @result 
	END

    RETURN @result
END

