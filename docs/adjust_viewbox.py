from lxml import etree
import sys


def modify_gantt(root):

    # Iterate over all rect elements
    for rect in root.iter("{http://www.w3.org/2000/svg}rect"):

        # Remove the rx and ry properties
        for attr in ["rx", "ry"]:
            if attr in rect.attrib:
                del rect.attrib[attr]

        # Check if the element has the milestone class
        if "class" in rect.attrib and "milestone" in str(rect.attrib["class"]):
            # Remove milestone from the class
            rect_class = str(rect.attrib["class"])
            rect_class = rect_class.replace("milestone", "").strip()
            rect.attrib["class"] = rect_class

            # Remove the transform-origin, rx and ry properties
            for attr in ["transform-origin", "rx", "ry"]:
                if attr in rect.attrib:
                    del rect.attrib[attr]

            # Change the height and width to 10
            rect.attrib["height"] = "10"
            rect.attrib["width"] = "10"

            # Add 5 to the x and y attribute
            rect.attrib["x"] = str(int(rect.attrib["x"]) + 5)
            rect.attrib["y"] = str(int(rect.attrib["y"]) + 5)

            rot_x = str(int(rect.attrib["x"]) + 5)
            rot_y = str(int(rect.attrib["y"]) + 5)

            # Add a transform property with a rotate function
            rect.attrib["transform"] = f"rotate(45,{rot_x},{rot_y})"


def modify_sequence(root):
    # Iterate over all g elements
    for g in root.iter("{http://www.w3.org/2000/svg}g"):
        # Iterate over all rect elements within each g element
        for rect in g.iter("{http://www.w3.org/2000/svg}rect"):
            if "class" in rect.attrib and "rect" in rect.attrib["class"]:
                g.remove(rect)


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
    x, y, width, height = map(lambda x: int(float(x)), viewBox.split())

    ariaRole = root.attrib.get("aria-roledescription")
    if ariaRole == "gantt":
        width -= 10
        height -= 30
        # Modify the SVG content
        modify_gantt(root)
    elif ariaRole == "flowchart-v2":
        # x += 7
        y += 49
        # width -= 13
        height -= 98
    elif ariaRole == "sequence":
        # Modify the SVG content
        modify_sequence(root)

    # Adjust the viewBox values
    x += x_adjust
    y += y_adjust
    width += width_adjust
    height += height_adjust

    # Cleanup namespaces
    # etree.cleanup_namespaces(root)

    # Set the adjusted viewBox attribute
    root.attrib["viewBox"] = f"{x} {y} {width} {height}"
    root.attrib["style"] = f"max-width: {width}px; background-color: white;"

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
