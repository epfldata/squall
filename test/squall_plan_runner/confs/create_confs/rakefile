$:.push Dir.getwd;
require 'util.rb'

$data_sizes = [0.1, 10]
$available_nodes = 220
$ackers = 17 #experimentally determined best value (at least in TPCH7 1G database)

$OUTPUT_PATH="generated"
$extension =  ".tbl"

$TOPOLOGY_NAME_PREFIX="username"
$storage_local_dir = "/tmp/ramdisk"
$storage_cluster_dir = "/data/squall_zone/storage"
$memory_size_mb = 4096

$dip_distributed = true
$dip_result_root = "../test/results/"

$datafiles = $data_sizes.map { |size| [size, "/data/squall_blade/data/tpchdb/" + size.to_s + "G"] }

$heuristics = 
[1, 2, 4, 8, 16].map do |parallelism|
  ["parallel_#{parallelism}", lambda do |q,nodes,sources|
    { 
      :node_parallelism      => nodes.map { |n,n_args| (parallelism * (2**(n_args.fetch(:depth)-2))).to_i }.
				   map { |node_par| (node_par == 0 ? 1 : node_par) },
      :source_parallelism    => sources.map{ |s,s_args| (s_args.fetch(:small) ? 1 : parallelism) }
    }
  end]
end


# CHANGE BETWEEN HERE AND ...

def memPossible7(sizestr, parallelstr)
  datasize = Float(sizestr)
  parallelism = parallelstr.scan(/\d+/)[0].to_i

  case datasize
  when 0 .. 4
    return true
  when 5 .. 8
    return parallelism >= 2
  when 9 .. 10
    return parallelism >= 4
  else
    return false
  end
end

def memPossible3(sizestr, parallelstr)
  datasize = Float(sizestr)
  parallelism = parallelstr.scan(/\d+/)[0].to_i

  case datasize
  when 0 .. 1
    return true
  when 2
    return parallelism >= 2
  when 3 .. 6
    return parallelism >= 4
  when 7 .. 10
    return parallelism >= 8
  else
    return false
  end
end


$all_queries = {
  "tpch7" => {
    :datafiles => $datafiles,
    :pruneMethod => method(:memPossible7),
    :delim => "|", :escaped_delim => "\\|",
    :sources => [ 
      ["NATION"                                         , { :small => true }], # if source is small, parallelism will always be 1
      ["CUSTOMER"                                       , { :small => false }],
      ["ORDERS"                                         , { :small => false }],
      ["SUPPLIER"                                       , { :small => false }],
      ["LINEITEM"                                       , { :small => false }]
    ],
    :nodes => [ 
      ["NATION_CUSTOMER"                                , { :depth => 1 }],
      ["NATION_CUSTOMER_ORDERS"                         , { :depth => 2 }],
      ["SUPPLIER_NATION"                                , { :depth => 1 }],
      ["LINEITEM_SUPPLIER_NATION"                       , { :depth => 2 }],
      ["NATION_CUSTOMER_ORDERS_LINEITEM_SUPPLIER_NATION", { :depth => 3 }]
    ]
  },
  "tpch3" => {
    :datafiles => $datafiles,
    :pruneMethod => method(:memPossible3),
    :delim => "|", :escaped_delim => "\\|",
    :sources => [ 
      ["CUSTOMER"                                       , { :small => false }],
      ["ORDERS"                                         , { :small => false }],
      ["LINEITEM"                                       , { :small => false }]
    ],
    :nodes => [ 
      ["CUSTOMER_ORDERS"                                , { :depth => 1 }],
      ["CUSTOMER_ORDERS_LINEITEM"			, { :depth => 2 }]
    ]
  }
}


$wanted_queries = ["tpch3", "tpch7"]
$queries = $all_queries.reject { |key,_| !$wanted_queries.include? key}

# ... AND HERE

$queries.each do |q, q_args|
  $heuristics.each do |h, heuristic|
    q_args[:datafiles].each do |size, datapath|
      size_str = size.to_s.gsub(".", "_") # Storm 0.8.0 does not support "." characters in a topology name
      config_name = "#{size_str}G_#{q}_#{h}"
       if q_args[:pruneMethod].call(size, h)
        File.open("#{$OUTPUT_PATH}/#{config_name}", "w+") do |f|
          delim         = q_args.fetch(:delim, ",");
          escaped_delim = q_args.fetch(:escaped_delim, delim);
          
          nodes = q_args.fetch(:nodes);
	  sources = q_args.fetch(:sources);
          h_results = heuristic.call(q, nodes, sources);

          node_parallelism = h_results.fetch(:node_parallelism);
	  source_parallelism = h_results.fetch(:source_parallelism);

          f.puts("# Auto-generated test script for #{config_name}

DIP_DISTRIBUTED #{$dip_distributed}
DIP_QUERY_NAME #{q}
DIP_TOPOLOGY_NAME_PREFIX #{$TOPOLOGY_NAME_PREFIX}

# the following two are optional, by default they use topology.workers and topology.ackers from storm.yaml
#{ 
   if $dip_distributed then
      "DIP_NUM_WORKERS #{$available_nodes}"
   end
}
DIP_NUM_ACKERS #{$ackers}

DIP_DATA_PATH #{datapath}
#{ 
   if !$dip_distributed then
      "DIP_RESULT_ROOT #{$dip_result_root}"
   end
}

#{
sources.unzip[0].zip(source_parallelism).map do |source,source_par| "
#{source}_PAR #{source_par}
"
end
}

#{
nodes.unzip[0].zip(node_parallelism).map do |node,node_par| "
#{node}_PAR #{node_par}
"
end
}

#below are unlikely to change
DIP_EXTENSION #{$extension}
DIP_READ_SPLIT_DELIMITER #{escaped_delim}
DIP_GLOBAL_ADD_DELIMITER #{delim}
DIP_GLOBAL_SPLIT_DELIMITER #{escaped_delim}

DIP_KILL_AT_THE_END true

# Storage manager parameters
# Storage directory for local runs
STORAGE_LOCAL_DIR #{$storage_local_dir}
# Storage directory for cluster runs
STORAGE_CLUSTER_DIR #{$storage_cluster_dir}
STORAGE_COLD_START true
STORAGE_MEMORY_SIZE_MB #{$memory_size_mb}

")
        end # File.open
        task :default => "#{$OUTPUT_PATH}/#{config_name}"	
      end # if q_args
    end # q_args do
  end # heuristic
end # queries