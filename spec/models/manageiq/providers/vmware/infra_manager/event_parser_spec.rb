require 'VMwareWebService/VimTypes'

describe ManageIQ::Providers::Vmware::InfraManager::EventParser do
  EPV_DATA_DIR = File.expand_path(File.join(File.dirname(__FILE__), "event_data"))

  context ".event_to_hash" do
    let(:ems) { FactoryBot.create(:ems_vmware) }

    it "with a missing eventType" do
      event = VimHash.new("Event")
      expect { described_class.event_to_hash(event, ems.id) }.to raise_error(MiqException::Error, "event must have an eventType")
    end

    it "with a missing chainId" do
      event = VimHash.new("Event")
      event.eventType = "VmEvent"
      expect { described_class.event_to_hash(event, ems.id) }.to raise_error(MiqException::Error, "event must have a chain_id")
    end

    it "with a GeneralUserEvent" do
      event = YAML.load_file(File.join(EPV_DATA_DIR, 'general_user_event.yml'))
      data = described_class.event_to_hash(event, ems.id)

      expect(data).to include(
        :event_type   => "GeneralUserEvent",
        :chain_id     => "5361104",
        :is_task      => false,
        :source       => "VC",
        :message      => "User logged event: EVM SmartState Analysis completed for VM [tch-UBUNTU-904-LTS-DESKTOP]",
        :timestamp    => "2010-08-24T01:08:10.396636Z",
        :ems_id       => ems.id,
        :username     => "MANAGEIQ\\thennessy",

        :vm_ems_ref   => "vm-106741",
        :vm_name      => "tch-UBUNTU-904-LTS-DESKTOP",
        :vm_location  => "[msan2] tch-UBUNTU-904-LTS-DESKTOP/tch-UBUNTU-904-LTS-DESKTOP.vmx",
        :host_ems_ref => "host-106569",
        :host_name    => "yoda.manageiq.com",
      )

      expect(data[:full_data]).to    eq(event)
      expect(data[:full_data]).to    be_instance_of Hash
      expect(data[:vm_ems_ref]).to   be_instance_of String
      expect(data[:host_ems_ref]).to be_instance_of String
    end

    context "with an EventEx event" do
      it "with an eventTypeId" do
        event = YAML.load_file(File.join(EPV_DATA_DIR, 'event_ex.yml'))
        data = described_class.event_to_hash(event, ems.id)

        assert_result_fields(data, event)
        expect(data).to include(
          :event_type => "vprob.vmfs.resource.corruptondisk",
          :message    => "event.vprob.vmfs.resource.corruptondisk.fullFormat (vprob.vmfs.resource.corruptondisk)"
        )
      end

      it "without an eventTypeId" do
        event = YAML.load_file(File.join(EPV_DATA_DIR, 'event_ex_without_eventtypeid.yml'))
        data = described_class.event_to_hash(event, ems.id)

        assert_result_fields(data, event)
        expect(data).to include(
          :event_type => "EventEx",
          :message    => ""
        )
      end

      def assert_result_fields(data, event)
        expect(data).to include(
          :chain_id     => "297179",
          :is_task      => false,
          :source       => "VC",
          :timestamp    => "2010-11-12T17:15:42.661128Z",
          :ems_id       => ems.id,
          :host_ems_ref => "host-29",
          :host_name    => "vi4esx1.galaxy.local",
        )

        expect(data[:full_data]).to    eq(event)
        expect(data[:full_data]).to    be_instance_of Hash
        expect(data[:host_ems_ref]).to be_instance_of String
      end
    end

    context "with a TaskEvent" do
      let(:event) { YAML.load_file(File.join(EPV_DATA_DIR, 'task_event.yaml')) }

      it "sets the vm_uid_ems" do
        expect(described_class.event_to_hash(event, 12_345)).to include(
          :vm_uid_ems => event["vm"]["uuid"]
        )
      end
    end
  end
end
