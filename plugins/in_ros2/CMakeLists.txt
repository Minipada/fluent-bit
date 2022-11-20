
set(src
  in_ros2.c
  )
add_library(flb-plugin-in_ros2 STATIC "${src}")
ament_target_dependencies(
  flb-plugin-in_ros2
  rclc
  dc_interfaces
)
add_sanitizers(flb-plugin-in_ros2)
target_link_libraries(flb-plugin-in_ros2 fluent-bit-static msgpack-c-static)
