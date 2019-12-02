class ManageIQ::Providers::Vmware::InfraManager::RefreshWorker::Runner < ManageIQ::Providers::BaseManager::RefreshWorker::Runner
  # When using streaming_refresh we don't need to use the VimBrokerWorker
  self.require_vim_broker           = !Settings.ems_refresh.vmwarews.streaming_refresh
  self.delay_startup_for_vim_broker = !Settings.ems_refresh.vmwarews.streaming_refresh

  def after_initialize
    self.ems = @emss = ExtManagementSystem.find(@cfg[:ems_id])
    do_exit("Unable to find instance for EMS id [#{@cfg[:ems_id]}].", 1) if @ems.nil?
  end

  def before_exit(_message, _exit_code)
    stop_inventory_collector if ems.supports_streaming_refresh?
  end

  def do_before_work_loop
    # No need to queue an initial full refresh if we are streaming
    return if ems.supports_streaming_refresh?

    log_prefix = "EMS [#{ems.hostname}] as [#{ems.authentication_userid}]"
    _log.info("#{log_prefix} Queueing initial refresh for EMS #{ems.id}.")
    EmsRefresh.queue_refresh(ems)
  end

  def do_work
    if ems.supports_streaming_refresh?
      ensure_inventory_collector
    elsif collector&.running?
      stop_inventory_collector
    end

    super
  end

  def deliver_queue_message(msg)
    if ems.supports_streaming_refresh? && refresh_queued?(msg)
      super do
        if full_refresh_queued?(msg)
          restart_inventory_collector
        else
          _log.info("Dropping refresh targets [#{msg.data}] because streaming refresh is enabled")
        end
      end
    else
      super
    end
  end

  private

  attr_accessor :ems, :collector

  def start_inventory_collector
    self.collector = ems.class::Inventory::Collector.new(ems)
    collector.start
    _log.info("Started inventory collector")
  end

  def ensure_inventory_collector
    return if collector&.running?

    _log.warn("Inventory collector thread not running, restarting...") unless collector.nil?
    start_inventory_collector
  end

  def stop_inventory_collector
    collector&.stop
    self.collector = nil
  end

  def restart_inventory_collector
    _log.info("Restarting inventory collector...")
    collector&.restart
    _log.info("Restarting inventory collector...Complete")
  end

  def refresh_queued?(msg)
    msg.class_name == "EmsRefresh" && msg.method_name == "refresh"
  end

  def full_refresh_queued?(msg)
    refresh_queued?(msg) && msg.data.any? { |klass, _id| klass == ems.class.name }
  end
end
