import tarfile
from pathlib import Path

p = "/Users/sanhehu/Documents/GitHub/aws_textract_pipeline-project/dist/aws_textract_pipeline-0.1.1.tar.gz"
with Path(p).open("rb") as fileobj:
    with tarfile.open(fileobj=fileobj, mode="r") as tar:
        file_members = [member for member in tar.getmembers() if member.isfile()]
        sorted_file_members = list(sorted(
            file_members,
            key=lambda x: x.name,
        ))
        for member in sorted_file_members:
            f = tar.extractfile(member)
            if f is not None:
                content = f.read()
                print(member.name, len(content))
