# This script runs all the config files from CONF_PATH, in order to obtain statistics, Component -> (time of execution, node, failed tuples and exceptions)

require 'time'
require 'gnuplot'
require 'util.rb'

task :extract, :mode, :base_path, :conf_path, :storm_data_path do |t, args|

$RELATIVE="../"; # How from the rake directory to come to bin directory
$BASE_PATH = $RELATIVE + args.base_path;
$CONF_PATH = $RELATIVE + args.conf_path;
$STORM_DATA_DIR = $RELATIVE + args.storm_data_path;
$TOPOLOGY_NAME_PREFIX="username";
$RAKE_OUTPUT="cluster_exec.info";

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
    write_file("#{$BASE_PATH}/#{config_name}/#{$RAKE_OUTPUT}", stat_dump)
end #$topology_names
end

task :default => [:extract]
