# Welcome to cosmetecor.org scripts page 

### Content

      scripts for paint graphs and etc

Path | Description
------------ | -------------
**\content\content.rb** | task manager for paint graphs over time

**\content\gen_image.rb**
      job generator for gnuplot. Use \content\gnuplot.rb

**\content\gnuplot.rb**	
      paint graph. Different graphs types

**\content\gen_image_desp.rb**	
      job generator for gnuplot by USER. Use \content\gnuplot_desp_new2017.rb

**\content\gnuplot_desp.rb**	
      paint graph user request. Old version

**\content\gnuplot_desp_new2017.rb**	paint graph user request. 
	

**\content\select_magnitude_7.rb**	
       paint circles with magnitude
	
**\content\paint8in1\select_4db.rb**	
      paint 8 graphs on 1 list

**\content\paint8in1\gnu.dat**	
      template for gnuplot
	
**\content\gen_earth_quake_kml.rb**	
      earthquakes map with google-maps
      
      \content\gen_graphs.rb	
      
      \content\gen_graphs_dev.rb	
      
      \content\gen_kml_graphs.rb	
      
      \content\gen_plate_boundaries.rb	

**\content\gen_kml_lightning.rb**	
    lightning map with google-maps


### Manager

      scripts for parse and load data to DB
	
**\manager\manager.rb**	
      task manager for start parse-scripts over time
	
**\manager\cycles.rb**	

**\manager\geomag.rb**	
       geomag from ftp.swpc.noaa.gov/pub/indices/DGD.txt

**\manager\gu1_bo1.rb**	
       gu1 and bo1 indexes

**\manager\hemi.rb**	
      ftp.swpc.noaa.gov/pub/lists/hpi/pwr_1day

**\manager\lightning.rb**	
      lightning from http://flash3.ess.washington.edu/USGS/AVO/archive/

**\manager\radiation_belt.rb**	
      from http://satdat.ngdc.noaa.gov/sem/poes/data/belt_indices/

**\manager\sunspot.rb**	
      from ftp.swpc.noaa.gov/pub/indices/DSD.txt
	
**\manager\usgs.rb**	
earthquakes from http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.csv

\manager\kam.rb	
      kamchatka eartquakes from http://emsd.ru/ts/all.php
	
**\manager\se_measurements.rb**	
      load files DATA-STATIONS from SERVER to DB
	
**\manager\http**  
    old script version
	
	\manager\http\geomag.rb	
	
	\manager\http\hemi.rb	
	
	\manager\http\sunspot.rb	
