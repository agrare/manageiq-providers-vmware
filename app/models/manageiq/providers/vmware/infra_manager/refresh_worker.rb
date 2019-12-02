class ManageIQ::Providers::Vmware::InfraManager::RefreshWorker < ManageIQ::Providers::BaseManager::RefreshWorker
  require_nested :Runner

  self.required_roles = %w[ems_inventory event]
end
