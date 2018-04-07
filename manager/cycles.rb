require 'sequel'
require 'logging'
require 'gnuplot'
require 'fileutils'

LINES_SIZE_WIDTH = 1536*1.5
LINES_SIZE_HEIGHT = 384*2
BOX_SIZE_WIDTH = BOX_SIZE_HEIGHT = 1024

$db = Sequel.mysql2(:user => 'root', :password => 'skif13', :database=>'db', :max_connections => 4)
Sequel.default_timezone = :utc

def gen_image_line cycles, bim, mg
  image = nil; count = nil; db = nil; mm = {}; sensor = nil
  arr_x = []
  arr_y = []
  bim_x = []; bim_y = []
  cycles.each do |line|
    arr_x << line[0]
    arr_y << line[1]
  end
  bim.each do |line|
    bim_x << line[:date]
    bim_y << line[:magnitude]
  end
  if $silence_gnuplot
    $std = $stderr.dup
    if RUBY_PLATFORM.downcase.index("x86_64-linux")
      $stderr.reopen '/dev/null'
    else
      $stderr.reopen 'NUL'
    end
    $stderr.sync = true
  end
  #freq = ((mm[:max]-mm[:min]) / 5).to_i
  freq = 5 #if freq <= 0
  # y1 = $db[$table].order(Sequel.asc(:date)).limit(1).first[:date].strftime("%Y/%m/%d")
  # y2 = $db[$table].order(Sequel.desc(:date)).limit(1).first[:date].strftime("%Y/%m/%d")
  name = "Cycles for Mg=#{mg[0]}-#{mg[1]}"
  Gnuplot.open do |gp|
    Gnuplot::Plot.new( gp ) do |plot|
      plot.terminal "png nocrop font \"arial,12\" fontscale 1.0 size #{LINES_SIZE_WIDTH / 2}, #{LINES_SIZE_HEIGHT}"
      plot.output "../web/public/cycles/cycles-#{mg[0]}-#{mg[1]}.png"
      plot.title  "#{name}"
      plot.ylabel "N/cycles"
      plot.xlabel "Date"
      plot.timefmt '"%Y/%m/%d"'
      plot.xdata 'time'
      plot.rmargin '5'
      plot.ytics "autofreq #{freq}"
      plot.format 'x "%m/%d"'
      #plot.xrange '["0:0:0":"24:0:0"]'
      plot.tics 'scale 2'
      #plot.xtics "#{3600}"
      plot.grid 'xtics ytics'
      plot.data << Gnuplot::DataSet.new( [arr_x, arr_y] ) do |ds|
        ds.with = "impulses"
        ds.linewidth = 1
        ds.notitle
        ds.using = '1:2'
      end
      plot.data << Gnuplot::DataSet.new( [bim_x, bim_y] ) do |ds|
        ds.with = "points pointtype 7 pointsize 2"
        #ds.linewidth = 4
        ds.notitle
        ds.using = '1:2'
      end
    end
    $stderr.reopen $std if $silence_gnuplot
  end
end

$silence_gnuplot = true#nil#

$table = :earth_usgs
def get_quakes mg, d1, d2
  quakes = $db[$table].order(:date).where{(magnitude >= mg[0]) & (magnitude < mg[1])}.
                       where{date >= d1}.where{date < d2}
  quakes.all
end

FileUtils.mkdir('../web/public/cycles') unless Dir.exists?('../web/public/cycles')

[[4, 5], [5, 6], [6, 7], [7, 8]].each do |mg|
  cycles = []
  d_end = $db[:earth_usgs].select(:date).order(Sequel.desc(:date)).limit(1).first[:date]
  d_left = (d_end - 365*3600*24).to_date + 1
  bim = $db[$table].select(:date, :magnitude).order(:date).where{magnitude >= 7}.
                    where{date >= d_left}.all
  bim.map!{|b| b = {:date => b[:date].strftime("%Y/%m/%d"), :magnitude => b[:magnitude]}}
  d_end = d_end.to_date
  while d_left < d_end - 7
    cik = 0
    d_r = d_left + 7
    floon = get_quakes mg, d_left, d_r
    count = floon.size
    while hop = floon.shift
      next if floon.size == 0
      floon.each do |bri|
        d = (Math::sin(hop[:latitude]/(180/Math::PI)) * Math::sin(bri[:latitude]/(180/Math::PI)) + Math::cos(hop[:latitude]/(180/Math::PI)) * Math::cos(bri[:latitude]/(180/Math::PI)) * Math::cos((hop[:longitude] - bri[:longitude])/(180/Math::PI)))
        d = 1 if d > 1
        d = -1 if d < -1
        dist = 6371 * Math::acos(d)
        if dist <= 150 # 150 km
          cik += 1
        end
      end
    end
    cycles << [d_left.strftime("%Y/%m/%d"), count.to_f/cik.to_f]
    d_left += 1
  end
  gen_image_line cycles, bim, mg
end