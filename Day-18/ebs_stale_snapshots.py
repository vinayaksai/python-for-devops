import boto3
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    ec2 = boto3.client('ec2')

    # 1. Get all EBS snapshots owned by this account
    snapshots_response = ec2.describe_snapshots(OwnerIds=['self'])
    snapshots = snapshots_response['Snapshots']

    # 2. Get all active EC2 instance IDs (running + stopped)
    instances_response = ec2.describe_instances(
        Filters=[
            {
                'Name': 'instance-state-name',
                'Values': ['running', 'stopped']
            }
        ]
    )

    active_instance_ids = set()
    for reservation in instances_response['Reservations']:
        for instance in reservation['Instances']:
            active_instance_ids.add(instance['InstanceId'])

    # 3. Iterate through each snapshot
    for snapshot in snapshots:
        snapshot_id = snapshot['SnapshotId']
        volume_id = snapshot.get('VolumeId')

        # Case 1: Snapshot not associated with any volume
        if not volume_id:
            ec2.delete_snapshot(SnapshotId=snapshot_id)
            print(f"Deleted snapshot {snapshot_id} (no associated volume)")
            continue

        # Case 2: Volume exists or not
        try:
            volume_response = ec2.describe_volumes(VolumeIds=[volume_id])
            volume = volume_response['Volumes'][0]
            attachments = volume.get('Attachments', [])

            # If volume exists but not attached to any instance
            if not attachments:
                ec2.delete_snapshot(SnapshotId=snapshot_id)
                print(f"Deleted snapshot {snapshot_id} (volume not attached)")
                continue

            # If attached, check whether attached instance still exists
            attached_instance_id = attachments[0]['InstanceId']
            if attached_instance_id not in active_instance_ids:
                ec2.delete_snapshot(SnapshotId=snapshot_id)
                print(f"Deleted snapshot {snapshot_id} (instance no longer exists)")

        except ClientError as e:
            # Volume does not exist
            if e.response['Error']['Code'] == 'InvalidVolume.NotFound':
                ec2.delete_snapshot(SnapshotId=snapshot_id)
                print(f"Deleted snapshot {snapshot_id} (volume deleted)")
            else:
                print(f"Error processing snapshot {snapshot_id}: {e}")
