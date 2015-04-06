# This script runs all the config files from CONF_PATH, in order to obtain statistics, Component -> (time of execution, node, failed tuples and exceptions)

require 'time'
require 'gnuplot'
require 'util.rb'

$RELATIVE="../"; # How from the rake directory to come to bin directory

task :extract_all, :mode, :base_path, :conf_path, :storm_data_path do |t, args|
  $CONF_PATH = $RELATIVE + args.conf_path;
  $topology_names = Dir.foreach($CONF_PATH).find_all{|file| file != "." && file != ".." };
  $topology_names.each do |config_name|
    process_topology(config_name,args.base_path,args.storm_data_path)
  end #$topology_names
end

task :extract_one, :mode, :base_path, :config_name, :storm_data_path do |t, args|
  process_topology(args.config_name,args.base_path,args.storm_data_path)
end

def process_topology(config_name, base_path, storm_data_path)
  $BASE_PATH = $RELATIVE + base_path;
  $STORM_DATA_DIR = $RELATIVE + storm_data_path;

  $TOPOLOGY_NAME_PREFIX="username";
  $RAKE_OUTPUT="cluster_exec.info";
  topo_dump_str = "StormWrapper \\[INFO\\] In total there is";


  full_config_name = $TOPOLOGY_NAME_PREFIX + "_" + config_name
    stat_dump = [];
    topo_dump_files = `grep -r "#{topo_dump_str}" #{$STORM_DATA_DIR}`.
      split(/\n/).map do |l|
        [$1, Time.parse($2)] if /^([^:]+):([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}) ([a-zA-Z]\.)*StormWrapper \[INFO\] In total there is/ =~ l; 
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
end

def write_file (filename, data)
  if (data == "") then 
    puts "Missing info about #{filename}";
    return; 
  end
  puts "Generating #{filename}"
  File.open(filename, "w") {|f| f.write(data);}
end
