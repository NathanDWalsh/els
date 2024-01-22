from lxml import etree
import sys


def adjust_viewbox(svg_content, x_adjust, y_adjust, width_adjust, height_adjust):
    # Remove ZWSP characters from the input SVG content
    svg_content = svg_content.replace("â€‹", "")

    # Parse the SVG content
    root = etree.fromstring(svg_content)

    # Get the viewBox attribute
    viewBox = root.attrib.get("viewBox")
    if viewBox is None:
        raise ValueError("SVG content does not have a viewBox attribute")

    # Split the viewBox attribute into a list of integers
    x, y, width, height = map(int, viewBox.split())

    # Adjust the viewBox values
    x += x_adjust
    y += y_adjust
    width += width_adjust
    height += height_adjust

    # Cleanup namespaces
    # etree.cleanup_namespaces(root)

    # Set the adjusted viewBox attribute
    root.attrib["viewBox"] = f"{x} {y} {width} {height}"

    # Generate the modified SVG content
    svg_content_modified = etree.tostring(root).decode("utf-8")

    # Print the modified SVG content to stdout
    sys.stdout.write(svg_content_modified)


# Get the adjustment values from command line arguments
x_adjust = int(sys.argv[1])
y_adjust = int(sys.argv[2])
width_adjust = int(sys.argv[3])
height_adjust = int(sys.argv[4])

# Read the SVG content from stdin and adjust the viewBox
adjust_viewbox(sys.stdin.read(), x_adjust, y_adjust, width_adjust, height_adjust)
