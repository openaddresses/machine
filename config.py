openaddr_dir = './openaddresses'  # relative path to the openaddresses git repo.
output_dir = './output'  # relative path to preferred output directory
workspace_dir = './workspace'  # relative path to workspace

statefile_path = './state.txt'

"""
Set this to true to use shapely to validate geometries, and attempt to fix them if broken.
This will increase the verbosity of the script, and could potentially lose some shapes 
during import.
"""
clean_geom = False
