class ManageIQ::Providers::Vmware::InfraManager::EventCatcher < ManageIQ::Providers::BaseManager::EventCatcher
  require_nested :Runner

  self.required_roles = []
end
