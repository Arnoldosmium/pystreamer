PACKAGE_VERSION=$(git describe --tags | sed -E 's/-([0-9]+)-g/+\1.h/')
echo Render version: $PACKAGE_VERSION

sed "s/@PACKAGE_VERSION@/$PACKAGE_VERSION/g" setup.template.py > setup.py
