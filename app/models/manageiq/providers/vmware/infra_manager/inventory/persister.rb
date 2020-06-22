class ManageIQ::Providers::Vmware::InfraManager::Inventory::Persister < ManageIQ::Providers::Inventory::Persister
  require_nested :Batch
  require_nested :Targeted

  def initialize_inventory_collections
    add_collection(infra, :customization_specs)
    add_collection(infra, :disks, :parent_inventory_collections => %i[vms_and_templates])
    add_collection(infra, :distributed_virtual_switches)
    add_collection(infra, :distributed_virtual_lans)
    add_collection(infra, :clusters)
    add_collection(infra, :ems_custom_attributes, :parent_inventory_collections => %i[vms_and_templates])
    add_collection(infra, :ems_extensions)
    add_collection(infra, :ems_folders)
    add_collection(infra, :ems_licenses)
    add_collection(infra, :ext_management_system)
    add_collection(infra, :guest_devices, :parent_inventory_collections => %i[vms_and_templates])
    add_collection(infra, :hardwares, :parent_inventory_collections => %i[vms_and_templates])
    add_collection(infra, :hosts)
    add_collection(infra, :host_hardwares)
    add_collection(infra, :host_guest_devices)
    add_collection(infra, :host_networks)
    add_collection(infra, :host_storages, :parent_inventory_collections => %i[storages]) do |builder|
      builder.add_properties(:arel => manager.host_storages.joins(:storage))
    end
    add_collection(infra, :host_switches)
    add_collection(infra, :host_system_services)
    add_collection(infra, :host_operating_systems)
    add_collection(infra, :host_virtual_switches)
    add_collection(infra, :host_virtual_lans)
    add_collection(infra, :miq_scsi_luns)
    add_collection(infra, :miq_scsi_targets)
    add_collection(infra, :networks, :parent_inventory_collections => %i[vms_and_templates])
    add_collection(infra, :operating_systems, :parent_inventory_collections => %i[vms_and_templates])
    add_collection(infra, :resource_pools)
    add_collection(infra, :snapshots, :parent_inventory_collections => %i[vms_and_templates])
    add_collection(infra, :storages)
    add_collection(infra, :storage_profiles)
    add_collection(infra, :storage_profile_storages)
    add_collection(infra, :parent_blue_folders)
    add_collection(infra, :vms_and_templates) do |builder|
      builder.vm_template_shared
      builder.add_properties(:custom_reconnect_block => vm_reconnect_block)
    end
    add_collection(infra, :vm_parent_blue_folders)
    add_collection(infra, :vm_resource_pools)
    add_collection(infra, :root_folder_relationship)
    add_collection(infra, :orchestration_templates)
  end

  def vim_class_to_collection(managed_object)
    case managed_object
    when RbVmomi::VIM::ComputeResource
      clusters
    when RbVmomi::VIM::Datacenter
      ems_folders
    when RbVmomi::VIM::HostSystem
      hosts
    when RbVmomi::VIM::Folder
      ems_folders
    when RbVmomi::VIM::ResourcePool
      resource_pools
    end
  end

  private

  def vm_reconnect_block
    lambda do |inventory_collection, inventory_objects_index, attributes_index|
      vm_uids                 = attributes_index.values.map { |vm| vm[:uid_ems] }.compact
      archived_vms_by_uid_ems = inventory_collection.model_class.where(:ems_id => nil, :uid_ems => vm_uids).group_by(&:uid_ems)

      inventory_objects_index.each do |ems_ref, inventory_object|
        possible_reconnects = archived_vms_by_uid_ems[attributes_index.dig(ems_ref, :uid_ems)]
        next if possible_reconnects.blank?

        inventory_object = inventory_objects_index.delete(ems_ref)
        hash             = attributes_index.delete(ems_ref)

        # Skip if hash is blank, which can happen when having several archived entities with the same ref
        next if hash.nil?

        # Prefer disconnected records with the same ems_ref if there are any
        found   = possible_reconnects.detect { |vm| vm.ems_ref == hash[:ems_ref] }
        found ||= possible_reconnects.first

        found.assign_attributes(hash.except(:id, :type))
        if !inventory_collection.check_changed? || found.changed?
          found.save!
          inventory_collection.store_updated_records(found)
        end

        inventory_object.id = found.id
      end
    end
  end
end
