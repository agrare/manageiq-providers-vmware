class ManageIQ::Providers::Vmware::InfraManager::Inventory::Collector
  include PropertyCollector
  include Vmdb::Logging

  def initialize(ems, threaded: true)
    @ems              = ems
    @exit_requested   = false
    @collector_thread = nil
  end

  def start
    self.collector_thread = start_collector_thread
  end

  def running?
    collector_thread&.alive?
  end

  def stop(join_timeout = 2.minutes)
    _log.info("#{log_header} Monitor updates thread exiting...")
    self.exit_requested = true
    return if join_timeout.nil?

    # The WaitOptions for WaitForUpdatesEx call sets maxWaitSeconds to 60 seconds
    collector_thread&.join(join_timeout)
  end

  def restart(join_timeout = 2.minutes)
    self.exit_requested = true
    collector_thread&.join(join_timeout)

    self.exit_requested   = false
    self.collector_thread = start_collector_thread
  end

  private

  attr_reader   :ems, :inventory_cache
  attr_accessor :exit_requested, :collector_thread

  def start_collector_thread
    Thread.new { collector }
  end

  def collector
    _log.info("#{log_header} Monitor updates thread started")

    vim = connect

    property_filter_by_role = {}
    role_by_property_filter = {}

    active_roles.each do |role|
      property_filter_by_role[role] = create_property_filter_for_role(vim, role)
      role_by_property_filter[property_filter_by_role[role]] = role
    end

    until exit_requested do
      version = monitor_updates(vim, version) do |updated_objects|
        updated_objects.each do |filter, object_set|
          _log.info("#{log_header} Got #{object_set.count} #{role_by_property_filter[filter]} updates")
        end
      end
    end
  rescue => err
    _log.error("#{log_header}: #{err}")
    _log.log_backtrace(err)
  ensure
    property_filter_by_role.values.each { |filter| destroy_property_filter(filter) }
    disconnect(vim)
  end

  def monitor_updates(vim, version = "")
    updated_objects = Hash.new { |hash, key| hash[key] = [] }

    begin
      update_set = wait_for_updates(vim, version)
      break if update_set.nil?

      version = update_set.version

      # Merge the update_set object_sets by filter until refresh can process
      # partial updates
      update_set.filterSet.each do |prop_filter_update|
        updated_objects[prop_filter_update.filter] += prop_filter_update.objectSet
      end
    end while update_set.truncated

    yield updated_objects

    return version
  end

  def connect
    host = ems.hostname
    username, password = ems.auth_user_pwd

    _log.info("#{log_header} Connecting to #{username}@#{host}...")

    vim_opts = {
      :ns       => 'urn:vim25',
      :host     => host,
      :ssl      => true,
      :insecure => true,
      :path     => '/sdk',
      :port     => 443,
      :rev      => '6.5',
    }

    require 'rbvmomi/vim'
    conn = RbVmomi::VIM.new(vim_opts).tap do |vim|
      vim.rev = vim.serviceContent.about.apiVersion
      vim.serviceContent.sessionManager.Login(:userName => username, :password => password)
    end

    _log.info("#{log_header} Connected")
    conn
  end

  def pbm_connect(vim)
    require "rbvmomi/pbm"
    RbVmomi::PBM.connect(vim, :insecure => true)
  end

  def disconnect(vim)
    return if vim.nil?

    vim.close
  end

  def wait_for_updates(vim, version)
    # Return if we don't receive any updates for 60 seconds break
    # so that we can check if we are supposed to exit
    options = RbVmomi::VIM.WaitOptions(:maxWaitSeconds => 60)

    vim.propertyCollector.WaitForUpdatesEx(:version => version, :options => options)
  end

  def log_header
    ""
  end

  def active_roles
    MiqServer.my_server.active_role_names & ManageIQ::Providers::Vmware::InfraManager::RefreshWorker.required_roles
  end
end
