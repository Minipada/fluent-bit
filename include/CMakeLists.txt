# Install fluent-bit headers
install(FILES "fluent-bit.h"
    DESTINATION ${FLB_INSTALL_INCLUDEDIR}
    COMPONENT headers
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

file(GLOB headers "fluent-bit/*.h")
install(FILES ${headers}
    DESTINATION ${FLB_INSTALL_INCLUDEDIR}/fluent-bit
    COMPONENT headers
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

file(GLOB headers "fluent-bit/tls/*.h")
install(FILES ${headers}
    DESTINATION ${FLB_INSTALL_INCLUDEDIR}/fluent-bit/tls/
    COMPONENT headers
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)


install(FILES "../lib/monkey/include/monkey/mk_core.h"
    DESTINATION ${FLB_INSTALL_INCLUDEDIR}/monkey/
    COMPONENT headers-extra
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

file(GLOB headers "../lib/monkey/include/monkey/mk_core/*.h")
    install(FILES ${headers}
    COMPONENT headers-extra
    DESTINATION ${FLB_INSTALL_INCLUDEDIR}/monkey/mk_core/
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

file(GLOB headers "../lib/flb_libco/*.h")
    install(FILES ${headers}
    COMPONENT headers-extra
    DESTINATION ${FLB_INSTALL_INCLUDEDIR}/
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

file(GLOB headers "../lib/msgpack-c/include/*.h")
install(FILES ${headers}
    DESTINATION ${FLB_INSTALL_INCLUDEDIR}
    COMPONENT headers-extra
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

file(GLOB headers "../lib/msgpack-c/include/msgpack/*.h")
install(FILES ${headers}
    DESTINATION ${FLB_INSTALL_INCLUDEDIR}/msgpack
    COMPONENT headers-extra
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

file(GLOB headers "${monkey_BINARY_DIR}/include/monkey/mk_core/*.h")
install(FILES ${headers}
  DESTINATION include/mk_core
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

file(GLOB headers "${monkey_BINARY_DIR}/include/monkey/*.h")
install(FILES ${headers}
  DESTINATION include
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

file(GLOB headers "${mbed_TLS_SOURCE_DIR}/include/mbedtls/*.h")
install(FILES ${headers}
  DESTINATION include/mbedtls
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

file(GLOB headers "${c-ares_SOURCE_DIR}/include/*.h")
install(FILES ${headers}
  DESTINATION include
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

file(GLOB headers "${cmetrics_SOURCE_DIR}/include/cmetrics/*.h")
install(FILES ${headers}
  DESTINATION include/cmetrics
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

file(GLOB headers "${cmetrics_SOURCE_DIR}/include/prometheus_remote_write/*.h")
install(FILES ${headers}
  DESTINATION include/prometheus_remote_write
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

