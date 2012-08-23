require 'time'
require 'gnuplot'
require 'util.rb'

task :extract do
  $TIMING_DIR = "timing_info";
  $GRAPHS_DIR = "graphs";
  
  $topology_name_prefix = "username"
  $queries    = ["tpch7"];
  $datasets   = ["0_1G", "1G"];
  $mode       = "nrl";
  $source_par = 10;
  
  $queries.each do |q|
    time_graph="#{$GRAPHS_DIR}/time_#{q}.pdf";
    Gnuplot.open do |gp|
        Gnuplot::Plot.new(gp) do |plot|
          plot.term "pdf";
	  plot.output time_graph
        
          i = 0; x_axes = $datasets.map {|m| [i+=1, m]};
          plot.xtics "(#{x_axes.map {|i,m| "'#{m}' #{i}"}.join(",")})";
        
	  data = 
	    x_axes.map do |i, ds|
	      config = "#{ds}_#{q}_#{$mode}_#{$source_par}"
              if /, ([0-9]+) uptime seconds./ =~
                File.open("#{$TIMING_DIR}/#{config}") {|f|f.readlines}.
                  join("\n")
              then    
                [i, $1];
              else
                puts "Missing datapoint for #{config}_#{m}"
                [i, 0];
              end # if
	    end # x_axes
	    
          plot.data << Gnuplot::DataSet.new(data.unzip) do |gp_ds|
            gp_ds.with = "lines";
            gp_ds.title = "#{q}_#{$mode}_#{$source_par}";
          end # plot.data
	    
      end # Gnuplot.new
    end # Gnuplot.open
  end # queries
end # task

task :default => [:extract]