PACKAGE_VERSION=$(git describe --tags | sed -E 's/-([0-9]+)-g.+/-rc\1/')

echo Render version: $PACKAGE_VERSION

sed "s/@PACKAGE_VERSION@/$PACKAGE_VERSION/g" setup.template.py > setup.py
