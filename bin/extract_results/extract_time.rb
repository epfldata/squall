# This script runs all the config files from CONF_PATH, in order to obtain statistics (timings and eventual errors are in separate file per configuration)

require 'time'
require 'gnuplot'
require 'util.rb'

task :extract, :mode do |t, args|

if (args.mode == "SQL") then
  CONF_MODE="squall"
else
  CONF_MODE="squall_plan_runner"
end

$CONF_PATH = "../../test/#{CONF_MODE}/confs/create_confs/generated";
$STORM_DATA_DIR = "data";
$TIMING_DIR = "timing_info";
$TOPOLOGY_NAME_PREFIX="username";

$topology_names = Dir.foreach($CONF_PATH).find_all{|file| file != "." && file != ".." };

def write_file (filename, data)
  if (data == "") then 
    puts "Missing info about #{filename}";
    return; 
  end
  puts "Generating #{filename}"
  File.open(filename, "w") {|f| f.write(data);}
end

topo_dump_str = "StormWrapper \\[INFO\\] In total there is";

$topology_names.each do |config_name|
    full_config_name = $TOPOLOGY_NAME_PREFIX + "_" + config_name
    stat_dump = [];
    topo_dump_files = `grep -r "#{topo_dump_str}" #{$STORM_DATA_DIR}`.
      split(/\n/).map do |l|
        [$1, Time.parse($2)] if /^([^:]+):([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}) StormWrapper \[INFO\] In total there is/ =~ l; 
      end.
      compact.
      reduce { |f,times| times.max }.
      to_a.
      sort { |a,b| a[1] <=> b[1] }.
      each do |file, time|
        File.open(file) do |f|
          appending = false;
          last_was_newline = false;
          f.each do |l|
            l.chomp!;
            if appending then
              if (l == "") && (last_was_newline) then
                appending = false;
              else
                stat_dump.push l;
              end
            elsif "For topology #{full_config_name}:" == l then
              appending = true;
              stat_dump = [];
            end
            last_was_newline = l == "";
          end
        end
      end
    stat_dump = stat_dump.join("\n");
    write_file("#{$TIMING_DIR}/#{config_name}", stat_dump)
end #$topology_names
end

task :default => [:extract]
