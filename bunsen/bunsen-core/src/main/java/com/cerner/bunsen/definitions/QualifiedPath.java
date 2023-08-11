package com.cerner.bunsen.definitions;

/** Simple immutable container for a path to an element qualified by the structure type. */
public class QualifiedPath {

  private final String parentTypeUrl;

  private final String elementPath;

  /**
   * Constructs a qualified path for the given element within the given parent type.
   *
   * @param parentTypeUrl the parent type containing the element.
   * @param elementPath the path of the element itself.
   */
  public QualifiedPath(String parentTypeUrl, String elementPath) {

    if (parentTypeUrl == null || elementPath == null) {
      throw new IllegalArgumentException(
          "The parentTypeUrl and elementPath arguments must not be null.");
    }

    this.parentTypeUrl = parentTypeUrl;
    this.elementPath = elementPath;
  }

  /**
   * Returns the URL of the parent type containing the element.
   *
   * @return the URL of the parent type containing the element.
   */
  public String getParentTypeUrl() {
    return parentTypeUrl;
  }

  /**
   * Returns the path of the element in parent type.
   *
   * @return the path of the element in the parent type.
   */
  public String getElementPath() {
    return elementPath;
  }

  @Override
  public int hashCode() {

    return 37 * parentTypeUrl.hashCode() * elementPath.hashCode();
  }

  @Override
  public boolean equals(Object object) {

    if (!(object instanceof QualifiedPath)) {
      return false;
    }

    QualifiedPath that = (QualifiedPath) object;

    return this.parentTypeUrl.equals(that.parentTypeUrl)
        && this.elementPath.equals(that.elementPath);
  }
}
