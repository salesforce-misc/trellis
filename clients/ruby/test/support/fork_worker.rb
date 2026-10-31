# frozen_string_literal: true

# Run by fork_test.rb: `ruby -I lib fork_worker.rb DSN`. A parent that
# connects and forks, the way a preloading, forking server would. Every
# process exits normally, so their at_exit hooks run and their handles are
# freed as Ruby exits.

require "trellis/pg"

$stdout.sync = true
dsn = ARGV.fetch(0)
Trellis.connect(url: dsn)

# Forked while the parent's handle is running: the child can neither use the
# handle it inherited, connect its own, nor render the metrics registry (whose
# lock it may have inherited held). It exits holding the inherited handle,
# which is left alone.
child = fork do
  begin
    Trellis.status("no_such_target")
    puts "child: status returned"
  rescue Trellis::ForkedHandleError => e
    puts "child: #{e.class}"
  end
  begin
    Trellis.connect(url: dsn)
    puts "child: connect returned"
  rescue Trellis::ForkedHandleError => e
    puts "child: connect #{e.class}, forked while running: " \
         "#{e.message.include?("was forked from process #{Process.ppid} while that process " \
                               'had a Trellis engine running')}"
  end
  begin
    Trellis::Metrics.render_prometheus
    puts "child: render_prometheus returned"
  rescue Trellis::ForkedHandleError => e
    puts "child: render_prometheus #{e.class}"
  end
  puts "child: connected? #{Trellis.connected?}"
end
Process.wait(child)
puts "child exit: #{$?.exitstatus}"

# The supported shape: shut down before forking (Puma's before_fork) and
# connect after (before_worker_boot).
Trellis.shutdown
worker = fork do
  Trellis.connect(url: dsn)
  puts "worker: own handle status #{Trellis.status('no_such_target').inspect}"
  puts "worker: connected? #{Trellis.connected?}"
  puts "worker: metrics name its instance " \
       "#{Trellis::Metrics.render_prometheus.include?('trellis_instance_up{trellis_instance=')}"
  # Exits without shutting down: the at_exit hook does it.
end
Process.wait(worker)
puts "worker exit: #{$?.exitstatus}"

# The parent can connect again once its workers are forked.
Trellis.connect(url: dsn)
puts "parent: status #{Trellis.status('no_such_target').inspect}"
# Exits without shutting down: the at_exit hook does it.
