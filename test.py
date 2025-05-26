from rclone_python import rclone

# items = rclone.ls("onedrive_test1:PhimHoatHinh/Yugi-Oh", max_depth=9999, args=["--hash"])
# print(items)


from daily_sync import get_cached_files, get_files_to_copy

# files = get_cached_files("GDrive60TB:PhimHoatHinh/Yugi-Oh", is_source=True)
# print(files)

files = get_files_to_copy("GDrive60TB:PhimHoatHinh/Yugi-Oh", "onedrive_test1:PhimHoatHinh/Yugi-Oh")
#print(files)