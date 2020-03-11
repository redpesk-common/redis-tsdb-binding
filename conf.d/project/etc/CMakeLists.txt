##################################################
# Control Policy Config file
##################################################
PROJECT_TARGET_ADD(redis-config)

	file(GLOB CONF_FILES "*.json")

	add_input_files("${CONF_FILES}")

	SET_TARGET_PROPERTIES(${TARGET_NAME} PROPERTIES
	LABELS "BINDING-CONFIG"
	OUTPUT_NAME ${TARGET_NAME}
	)
